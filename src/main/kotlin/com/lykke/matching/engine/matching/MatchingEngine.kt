package com.lykke.matching.engine.matching

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.CopyWrapper
import com.lykke.matching.engine.daos.LkkTrade
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.Order
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.fee.FeeException
import com.lykke.matching.engine.fee.FeeProcessor
import com.lykke.matching.engine.fee.NotEnoughFundsFeeException
import com.lykke.matching.engine.fee.singleFeeTransfer
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.outgoing.messages.LimitOrderWithTrades
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.outgoing.messages.LimitTradeInfo
import com.lykke.matching.engine.outgoing.messages.TradeInfo
import com.lykke.matching.engine.outgoing.messages.v2.builders.bigDecimalToString
import com.lykke.matching.engine.outgoing.messages.v2.enums.TradeRole
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.utils.NumberUtils
import org.apache.log4j.Logger
import java.math.BigDecimal
import java.util.Date
import java.util.HashMap
import java.util.HashSet
import java.util.LinkedList
import java.util.UUID
import java.util.concurrent.PriorityBlockingQueue

class MatchingEngine(private val LOGGER: Logger,
                     private val genericLimitOrderService: GenericLimitOrderService,
                     private val assetsHolder: AssetsHolder,
                     private val assetsPairsHolder: AssetsPairsHolder,
                     private val balancesHolder: BalancesHolder,
                     private val feeProcessor: FeeProcessor) {

    companion object {
        private const val RELATIVE_SPREAD_ACCURACY = 4
    }

    private var tradeIndex: Long = 0
    private val changedOrders = HashMap<LimitOrder, CopyWrapper<LimitOrder>>()

    fun initTransaction(): MatchingEngine {
        tradeIndex = 0
        changedOrders.clear()
        return this
    }

    fun apply() {
        val copyWrappers = changedOrders.values.toList()
        changedOrders.clear()
        copyWrappers.forEach { it.applyToOrigin() }
    }

    fun updatedOrders(orderBook: Collection<LimitOrder>, newOrders: Collection<LimitOrder>): UpdatedOrders {
        val updatedOrderBook = ArrayList<LimitOrder>(orderBook.size)
        val updatedOrders = newOrders.toMutableSet()
        orderBook.forEach { order ->
            val updatedOrder = changedOrders[order]?.copy
            if (updatedOrder != null) {
                updatedOrders.add(updatedOrder)
                updatedOrderBook.add(updatedOrder)
            } else {
                updatedOrderBook.add(order)
            }
        }
        return UpdatedOrders(updatedOrderBook, updatedOrders)
    }

    fun match(originOrder: Order,
              orderBook: PriorityBlockingQueue<LimitOrder>,
              messageId: String,
              balance: BigDecimal? = null,
              priceDeviationThreshold: BigDecimal? = null): MatchingResult {
        val orderWrapper = CopyWrapper(originOrder)
        val order = orderWrapper.copy
        val availableBalance = balance ?: getBalance(order)
        val workingOrderBook = PriorityBlockingQueue(orderBook)
        val bestPrice = if (workingOrderBook.isNotEmpty()) workingOrderBook.peek().takePrice() else null
        val now = Date()

        if (order.takePrice() != null && !checkExecutionPriceDeviation(order.isBuySide(), order.takePrice()!!, bestPrice, priceDeviationThreshold)) {
            LOGGER.info("Too high price deviation (order id: ${order.externalId}): threshold: $priceDeviationThreshold, bestPrice: $bestPrice, price: ${order.takePrice()})")
            order.updateStatus(OrderStatus.TooHighPriceDeviation, now)
            return MatchingResult(orderWrapper, now, emptySet())
        }

        var remainingVolume = order.getAbsVolume()
        val matchedOrders = LinkedList<CopyWrapper<LimitOrder>>()
        val skipLimitOrders = HashSet<LimitOrder>()
        val cancelledLimitOrders = HashSet<CopyWrapper<LimitOrder>>()
        var totalLimitPrice = BigDecimal.ZERO
        var totalVolume = BigDecimal.ZERO
        val limitReservedBalances = HashMap<String, BigDecimal>() // limit reserved balances for trades funds control
        val availableBalances = HashMap<String, MutableMap<String, BigDecimal>>() // clientId -> assetId -> balance; available balances for market balance control and fee funds control
        val assetPair = assetsPairsHolder.getAssetPair(order.assetPairId)
        val isBuy = order.isBuySide()
        val lkkTrades = LinkedList<LkkTrade>()
        val completedLimitOrders = LinkedList<CopyWrapper<LimitOrder>>()
        var matchedUncompletedLimitOrderWrapper: CopyWrapper<LimitOrder>? = null
        var uncompletedLimitOrderWrapper: CopyWrapper<LimitOrder>? = null
        val allOwnCashMovements = LinkedList<WalletOperation>()
        val allOppositeCashMovements = LinkedList<WalletOperation>()
        val asset = assetsHolder.getAsset(if (isBuy) assetPair.quotingAssetId else assetPair.baseAssetId)
        val limitAsset = assetsHolder.getAsset(if (isBuy) assetPair.baseAssetId else assetPair.quotingAssetId)

        setMarketBalance(availableBalances, order, asset, availableBalance)

        val marketOrderTrades = LinkedList<TradeInfo>()

        val limitOrdersReport = LimitOrdersReport(messageId)
        var totalLimitVolume = BigDecimal.ZERO
        var matchedWithZeroLatestTrade = false

        if (checkOrderBook(order, workingOrderBook)) {
            while (getMarketBalance(availableBalances, order, asset) >= BigDecimal.ZERO
                    && workingOrderBook.size > 0
                    && !NumberUtils.equalsWithDefaultDelta(remainingVolume, BigDecimal.ZERO)
                    && !matchedWithZeroLatestTrade
                    && (order.takePrice() == null || (if (isBuy) order.takePrice()!! >= workingOrderBook.peek().price else order.takePrice()!! <= workingOrderBook.peek().price))) {
                val limitOrderOrigin = workingOrderBook.poll()
                if (limitOrderOrigin.isExpired(now)) {
                    LOGGER.info("Added order (id: ${limitOrderOrigin.externalId}) to cancelled limit orders due to expired time")
                    cancelledLimitOrders.add(CopyWrapper(limitOrderOrigin))
                    continue
                }
                if (order.clientId == limitOrderOrigin.clientId) {
                    skipLimitOrders.add(limitOrderOrigin)
                    continue
                }

                val limitOrderCopyWrapper = changedOrders.getOrPut(limitOrderOrigin) { CopyWrapper(limitOrderOrigin) }
                val limitOrder = limitOrderCopyWrapper.copy

                var isFullyMatched = false

                val limitRemainingVolume = limitOrder.getAbsRemainingVolume()
                val marketRemainingVolume = getCrossVolume(remainingVolume, order.isStraight(), limitOrder.price)
                val volume = if (marketRemainingVolume > limitRemainingVolume) limitRemainingVolume else { isFullyMatched = true; marketRemainingVolume}


                var marketRoundedVolume = NumberUtils.setScale(if (isBuy) volume else -volume, assetsHolder.getAsset(assetPair.baseAssetId).accuracy, !isBuy)
                var oppositeRoundedVolume = NumberUtils.setScale(if (isBuy) -limitOrder.price * volume else limitOrder.price * volume, assetsHolder.getAsset(assetPair.quotingAssetId).accuracy, isBuy)

                LOGGER.info("Matching with limit order ${limitOrder.externalId}, client ${limitOrder.clientId}, price ${limitOrder.price}, " +
                        "marketVolume ${NumberUtils.roundForPrint(if (isBuy) oppositeRoundedVolume else marketRoundedVolume)}, " +
                        "limitVolume ${NumberUtils.roundForPrint(if (isBuy) marketRoundedVolume else oppositeRoundedVolume)}")

                if ((!order.isStraight()) && isFullyMatched) {
                    oppositeRoundedVolume = BigDecimal.valueOf(order.volume.signum().toLong()) * (NumberUtils.setScale(order.volume.abs() - totalLimitVolume.abs(), assetsHolder.getAsset(assetPair.quotingAssetId).accuracy, isBuy))
                    marketRoundedVolume = NumberUtils.setScale( NumberUtils.divideWithMaxScale(-oppositeRoundedVolume, limitOrder.price), assetsHolder.getAsset(assetPair.baseAssetId).accuracy, !isBuy)
                    LOGGER.info("Rounding last matched limit order trade: ${NumberUtils.roundForPrint(marketRoundedVolume)}")
                }

                LOGGER.info("Corrected volumes: " +
                        "marketVolume ${NumberUtils.roundForPrint(if (isBuy) oppositeRoundedVolume else marketRoundedVolume)}, " +
                        "limitVolume ${NumberUtils.roundForPrint(if (isBuy) marketRoundedVolume else oppositeRoundedVolume)}")

                val limitOrderInfo = "id: ${limitOrder.externalId}, client: ${limitOrder.clientId}, asset: ${limitOrder.assetPairId}"

                if (!genericLimitOrderService.checkAndReduceBalance(
                        limitOrder,
                        if (isBuy) marketRoundedVolume else oppositeRoundedVolume,
                        limitReservedBalances)) {
                    LOGGER.info("Added order ($limitOrderInfo) to cancelled limit orders")
                    cancelledLimitOrders.add(limitOrderCopyWrapper)
                    continue
                }

                if (BigDecimal.ZERO.compareTo(if (isBuy) marketRoundedVolume else oppositeRoundedVolume) == 0) {
                    if (isFullyMatched) {
                        LOGGER.info("Skipped order ($limitOrderInfo) due to zero latest trade")
                        matchedWithZeroLatestTrade = true
                        skipLimitOrders.add(limitOrderOrigin)
                    } else {
                        LOGGER.info("Added order ($limitOrderInfo) to cancelled limit orders due to zero trade")
                        cancelledLimitOrders.add(limitOrderCopyWrapper)
                    }
                    continue
                }

                if (NumberUtils.equalsIgnoreScale(BigDecimal.ZERO, if (isBuy) marketRoundedVolume else oppositeRoundedVolume)) {
                    if (isFullyMatched) {
                        LOGGER.info("Skipped order ($limitOrderInfo) due to zero latest trade")
                        matchedWithZeroLatestTrade = true
                        skipLimitOrders.add(limitOrderOrigin)
                    } else {
                        LOGGER.info("Added order ($limitOrderInfo) to cancelled limit orders due to zero trade")
                        cancelledLimitOrders.add(limitOrderCopyWrapper)
                    }
                    continue
                }

                val baseAssetOperation = WalletOperation(UUID.randomUUID().toString(), null, order.clientId, assetPair.baseAssetId, now, marketRoundedVolume, BigDecimal.ZERO)
                val quotingAssetOperation = WalletOperation(UUID.randomUUID().toString(), null, order.clientId, assetPair.quotingAssetId, now, oppositeRoundedVolume, BigDecimal.ZERO)
                val limitBaseAssetOperation = WalletOperation(UUID.randomUUID().toString(), null, limitOrder.clientId, assetPair.baseAssetId, now, -marketRoundedVolume, if (-marketRoundedVolume < BigDecimal.ZERO) -marketRoundedVolume else BigDecimal.ZERO)
                val limitQuotingAssetOperation = WalletOperation(UUID.randomUUID().toString(), null, limitOrder.clientId, assetPair.quotingAssetId, now, -oppositeRoundedVolume, if (-oppositeRoundedVolume < BigDecimal.ZERO) -oppositeRoundedVolume else BigDecimal.ZERO)

                val ownCashMovements = mutableListOf(baseAssetOperation, quotingAssetOperation)
                val oppositeCashMovements = mutableListOf(limitBaseAssetOperation, limitQuotingAssetOperation)

                val bestAsk = if (isBuy) limitOrder.price else genericLimitOrderService.getOrderBook(limitOrder.assetPairId).getAskPrice()
                val bestBid = if (isBuy) genericLimitOrderService.getOrderBook(limitOrder.assetPairId).getBidPrice() else limitOrder.price
                val validSpread = bestAsk > BigDecimal.ZERO && bestBid > BigDecimal.ZERO
                val absoluteSpread = if (validSpread) bestAsk - bestBid else null
                val relativeSpread = if (validSpread) NumberUtils.divideWithMaxScale(absoluteSpread!!, bestAsk) else null

                val makerFees = try {
                    feeProcessor.processMakerFee(limitOrder.fees ?: emptyList(),
                            if (isBuy) limitQuotingAssetOperation else limitBaseAssetOperation,
                            oppositeCashMovements,
                            relativeSpread,
                            mapOf(Pair(assetPair.assetPairId, limitOrder.price)),
                            availableBalances)
                } catch (e: FeeException) {
                    LOGGER.info("Added order ($limitOrderInfo) to cancelled limit orders: ${e.message}")
                    cancelledLimitOrders.add(limitOrderCopyWrapper)
                    continue
                }

                val takerFees = try {
                    feeProcessor.processFee(order.fees ?: emptyList(), if (isBuy) baseAssetOperation else quotingAssetOperation, ownCashMovements, mapOf(Pair(assetPair.assetPairId, limitOrder.price)), availableBalances)
                } catch (e: NotEnoughFundsFeeException) {
                    order.updateStatus(OrderStatus.NotEnoughFunds, now)
                    LOGGER.info("Not enough funds for fee for order id: ${order.externalId}, client: ${order.clientId}, asset: ${order.assetPairId}, volume: ${NumberUtils.roundForPrint(order.volume)}, price: ${order.takePrice()}, marketBalance: ${getMarketBalance(availableBalances, order, asset)} : ${e.message}")
                    return MatchingResult(orderWrapper, now, cancelledLimitOrders)
                } catch (e: FeeException) {
                    order.updateStatus(OrderStatus.InvalidFee, now)
                    LOGGER.info("Invalid fee for order id: ${order.externalId}, client: ${order.clientId}, asset: ${order.assetPairId}, volume: ${NumberUtils.roundForPrint(order.volume)}, price: ${order.takePrice()}, marketBalance: ${getMarketBalance(availableBalances, order, asset)} : ${e.message}")
                    return MatchingResult(orderWrapper, now, cancelledLimitOrders)
                }
                if (takerFees.isNotEmpty()) {
                    LOGGER.info("Taker fee transfers: ${takerFees.map { it.transfer }}")
                }
                if (makerFees.isNotEmpty()) {
                    LOGGER.info("Maker fee transfers: ${makerFees.map { it.transfer }}")
                }

                val matchedLimitOrderCopyWrapper = CopyWrapper(limitOrder)
                val limitOrderCopy = matchedLimitOrderCopyWrapper.copy
                if (limitOrderCopy.reservedLimitVolume != null && limitOrderCopy.reservedLimitVolume!! > BigDecimal.ZERO) {
                    limitOrderCopy.reservedLimitVolume =  NumberUtils.setScaleRoundHalfUp(limitOrderCopy.reservedLimitVolume!! + if (-marketRoundedVolume < BigDecimal.ZERO) -marketRoundedVolume else -oppositeRoundedVolume, limitAsset.accuracy)
                }

                val limitVolumeAsset = assetsHolder.getAsset(assetsPairsHolder.getAssetPair(limitOrder.assetPairId).baseAssetId)
                val newRemainingVolume = NumberUtils.setScaleRoundHalfUp(limitOrderCopy.remainingVolume + marketRoundedVolume, limitVolumeAsset.accuracy)
                val isLimitMatched = newRemainingVolume.signum() != limitOrderCopy.remainingVolume.signum()
                if (isLimitMatched) {
                    if (newRemainingVolume.signum() * limitOrderCopy.remainingVolume.signum() < 0) {
                        LOGGER.info("Matched volume is overflowed (previous: ${limitOrderCopy.remainingVolume}, current: $newRemainingVolume)")
                    }
                    lkkTrades.add(LkkTrade(limitOrder.assetPairId, limitOrder.clientId, limitOrder.price, limitOrderCopy.remainingVolume, now))
                    lkkTrades.add(LkkTrade(limitOrder.assetPairId, order.clientId, limitOrder.price, -limitOrderCopy.remainingVolume, now))
                    limitOrderCopy.remainingVolume = BigDecimal.ZERO
                    limitOrderCopy.updateStatus(OrderStatus.Matched, now)
                    completedLimitOrders.add(limitOrderCopyWrapper)
                    if (limitOrderCopy.reservedLimitVolume != null && limitOrderCopy.reservedLimitVolume!! > BigDecimal.ZERO) {
                        oppositeCashMovements.add(WalletOperation(UUID.randomUUID().toString(), null, limitOrder.clientId, if (-marketRoundedVolume < BigDecimal.ZERO) assetPair.baseAssetId else assetPair.quotingAssetId, now, BigDecimal.ZERO, -limitOrderCopy.reservedLimitVolume!!))
                        limitOrderCopy.reservedLimitVolume =  BigDecimal.ZERO
                    }
                } else {
                    lkkTrades.add(LkkTrade(limitOrder.assetPairId, limitOrder.clientId, limitOrder.price, -marketRoundedVolume, now))
                    lkkTrades.add(LkkTrade(limitOrder.assetPairId, order.clientId, limitOrder.price, marketRoundedVolume, now))
                    limitOrderCopy.remainingVolume = newRemainingVolume
                    limitOrderCopy.updateStatus(OrderStatus.Processing, now)
                    matchedUncompletedLimitOrderWrapper = matchedLimitOrderCopyWrapper
                    uncompletedLimitOrderWrapper = limitOrderCopyWrapper
                }

                setMarketBalance(availableBalances, order, asset, NumberUtils.setScaleRoundHalfUp(getMarketBalance(availableBalances, order, asset) - (if (isBuy) oppositeRoundedVolume else marketRoundedVolume).abs(), asset.accuracy))

                remainingVolume = if (isFullyMatched) BigDecimal.ZERO else NumberUtils.setScale(remainingVolume - getVolume(marketRoundedVolume.abs(), order.isStraight(), limitOrder.price), assetsHolder.getAsset(if (order.isStraight()) assetPair.baseAssetId else assetPair.quotingAssetId).accuracy, order.isOrigBuySide())
                limitOrderCopy.lastMatchTime = now

                allOppositeCashMovements.addAll(oppositeCashMovements)
                allOwnCashMovements.addAll(ownCashMovements)
                val traderId = UUID.randomUUID().toString()

                val roundedAbsoluteSpread = if (absoluteSpread != null) NumberUtils.setScaleRoundHalfUp(absoluteSpread, assetPair.accuracy) else null
                val roundedRelativeSpread = if (relativeSpread != null) NumberUtils.setScaleRoundHalfUp(relativeSpread, RELATIVE_SPREAD_ACCURACY) else null

                val baseAsset: Asset
                val quotingAsset: Asset
                val baseMarketVolume: String
                val baseLimitVolume: String
                val quotingMarketVolume: String
                val quotingLimitVolume: String
                if (isBuy) {
                    baseAsset = limitAsset
                    quotingAsset = asset
                    baseMarketVolume = bigDecimalToString(marketRoundedVolume.abs())!!
                    quotingMarketVolume = bigDecimalToString(-oppositeRoundedVolume.abs())!!
                    baseLimitVolume = bigDecimalToString(-marketRoundedVolume.abs())!!
                    quotingLimitVolume = bigDecimalToString(oppositeRoundedVolume.abs())!!
                } else {
                    baseAsset = asset
                    quotingAsset = limitAsset
                    baseMarketVolume = bigDecimalToString(-marketRoundedVolume.abs())!!
                    quotingMarketVolume = bigDecimalToString(oppositeRoundedVolume.abs())!!
                    baseLimitVolume = bigDecimalToString(marketRoundedVolume.abs())!!
                    quotingLimitVolume = bigDecimalToString(-oppositeRoundedVolume.abs())!!
                }

                marketOrderTrades.add(TradeInfo(traderId,
                        order.clientId,
                        NumberUtils.setScaleRoundHalfUp((if (isBuy) oppositeRoundedVolume else marketRoundedVolume).abs(), asset.accuracy).toPlainString(),
                        asset.assetId,
                        limitOrder.clientId,
                        NumberUtils.setScaleRoundHalfUp((if (isBuy) marketRoundedVolume else oppositeRoundedVolume).abs(), limitAsset.accuracy).toPlainString(),
                        limitAsset.assetId,
                        limitOrder.price,
                        limitOrder.id,
                        limitOrder.externalId,
                        now,
                        tradeIndex,
                        order.fee,
                        singleFeeTransfer(order.fee, takerFees),
                        takerFees,
                        roundedAbsoluteSpread,
                        roundedRelativeSpread,
                        baseAsset.assetId,
                        baseMarketVolume,
                        quotingAsset.assetId,
                        quotingMarketVolume))

                limitOrdersReport.orders.add(LimitOrderWithTrades(limitOrder,
                        mutableListOf(LimitTradeInfo(traderId,
                                limitOrder.clientId,
                                limitAsset.assetId,
                                NumberUtils.setScaleRoundHalfUp((if (isBuy) marketRoundedVolume else oppositeRoundedVolume).abs(), limitAsset.accuracy).toPlainString(),
                                limitOrder.price,
                                now,
                                order.id,
                                order.externalId,
                                asset.assetId,
                                order.clientId,
                                NumberUtils.setScaleRoundHalfUp((if (isBuy) oppositeRoundedVolume else marketRoundedVolume).abs(), asset.accuracy).toPlainString(),
                                tradeIndex,
                                limitOrder.fee,
                                singleFeeTransfer(limitOrder.fee, makerFees),
                                makerFees,
                                roundedAbsoluteSpread,
                                roundedRelativeSpread,
                                TradeRole.MAKER,
                                baseAsset.assetId,
                                baseLimitVolume,
                                quotingAsset.assetId,
                                quotingLimitVolume))))
                tradeIndex++

                totalVolume += volume
                totalLimitPrice += volume * limitOrder.price
                totalLimitVolume += (if (order.isStraight()) marketRoundedVolume else oppositeRoundedVolume).abs()
                matchedOrders.add(matchedLimitOrderCopyWrapper)
            }
        }

        if (order.takePrice() == null && remainingVolume > BigDecimal.ZERO) {
            if (matchedWithZeroLatestTrade) {
                order.updateStatus(OrderStatus.InvalidVolumeAccuracy, now)
                LOGGER.info("Invalid volume accuracy, latest trade has volume=0 for market order id: ${order.externalId}")
            } else {
                order.updateStatus(OrderStatus.NoLiquidity, now)
                LOGGER.info("No liquidity, not enough funds on limit orders, for market order id: ${order.externalId}}, client: ${order.clientId}, asset: ${order.assetPairId}, volume: ${NumberUtils.roundForPrint(order.volume)} | Unfilled: ${NumberUtils.roundForPrint(remainingVolume)}, price: ${order.takePrice()}")
            }
            return MatchingResult(orderWrapper, now, cancelledLimitOrders)
        }

        if (order.calculateReservedVolume() > availableBalance) {
            order.updateStatus(OrderStatus.ReservedVolumeGreaterThanBalance, now)
            LOGGER.info("Reserved volume (${order.calculateReservedVolume()}) greater than balance ($availableBalance) for order id: ${order.externalId}, client: ${order.clientId}, asset: ${order.assetPairId}, volume: ${NumberUtils.roundForPrint(order.volume)}, price: ${order.takePrice()}")
            return MatchingResult(orderWrapper, now, cancelledLimitOrders)
        }

        val reservedBalance = if (order.calculateReservedVolume() > BigDecimal.ZERO)  NumberUtils.setScale(order.calculateReservedVolume(), asset.accuracy, true) else availableBalance
        val marketBalance = getMarketBalance(availableBalances, order, asset)
        if (marketBalance < BigDecimal.ZERO  || reservedBalance < NumberUtils.setScale((if (isBuy) totalLimitPrice else totalVolume), asset.accuracy, true)) {
            order.updateStatus(OrderStatus.NotEnoughFunds, now)
            LOGGER.info("Not enough funds for order id: ${order.externalId}, " +
                    "client: ${order.clientId}, asset: ${order.assetPairId}, " +
                    "volume: ${NumberUtils.roundForPrint(order.volume)}, price: ${order.takePrice()}, " +
                    "marketBalance: $marketBalance : $reservedBalance < ${NumberUtils.setScaleRoundUp((if(isBuy) totalLimitPrice else totalVolume), asset.accuracy)}")
            return MatchingResult(orderWrapper, now, cancelledLimitOrders)
        }

        val executionPrice = calculateExecutionPrice(order, assetPair, totalLimitPrice, totalVolume)
        if (!checkMaxVolume(order, assetPair, executionPrice)) {
            order.updateStatus(OrderStatus.InvalidVolume, now)
            LOGGER.info("Too large volume of market order (${order.externalId}): volume=${order.volume}, price=$executionPrice, maxValue=${assetPair.maxValue}, straight=${order.isStraight()}")
            return MatchingResult(orderWrapper, now, cancelledLimitOrders)
        }
        if (!checkMaxValue(order, assetPair, executionPrice)) {
            order.updateStatus(OrderStatus.InvalidValue, now)
            LOGGER.info("Too large value of market order (${order.externalId}): volume=${order.volume}, price=$executionPrice, maxValue=${assetPair.maxValue}, straight=${order.isStraight()}")
            return MatchingResult(orderWrapper, now, cancelledLimitOrders)
        }
        if (order.takePrice() == null && !checkExecutionPriceDeviation(order.isBuySide(), executionPrice, bestPrice, priceDeviationThreshold)) {
            order.updateStatus(OrderStatus.TooHighPriceDeviation, now)
            LOGGER.info("Too high price deviation (order id: ${order.externalId}): threshold: $priceDeviationThreshold, bestPrice: $bestPrice, executionPrice: $executionPrice)")
            return MatchingResult(orderWrapper, now, cancelledLimitOrders)
        }

        if (order.takePrice() != null && remainingVolume > BigDecimal.ZERO) {
            val newRemainingVolume = if (order.isBuySide() || NumberUtils.equalsIgnoreScale(remainingVolume, BigDecimal.ZERO)) remainingVolume else -remainingVolume
            if (newRemainingVolume.compareTo(originOrder.volume) != 0) {
                order.updateStatus(OrderStatus.Processing, now)
                order.updateRemainingVolume(newRemainingVolume)
            }
        } else {
            order.updateStatus(OrderStatus.Matched, now)
            order.updateRemainingVolume(BigDecimal.ZERO)
        }
        order.updateMatchTime(now)
        order.updatePrice(executionPrice)

        return MatchingResult(orderWrapper,
                now,
                cancelledLimitOrders,
                matchedOrders,
                skipLimitOrders,
                completedLimitOrders,
                matchedUncompletedLimitOrderWrapper,
                uncompletedLimitOrderWrapper,
                lkkTrades,
                allOwnCashMovements,
                allOppositeCashMovements,
                marketOrderTrades,
                limitOrdersReport,
                workingOrderBook,
                marketBalance,
                matchedWithZeroLatestTrade,
                false)
    }

    private fun checkOrderBook(order: Order, orderBook: PriorityBlockingQueue<LimitOrder>): Boolean =
            orderBook.isEmpty() || orderBook.peek().assetPairId == order.assetPairId && orderBook.peek().isBuySide() != order.isBuySide()

    private fun getCrossVolume(volume: BigDecimal, straight: Boolean, price: BigDecimal): BigDecimal {
        return if (straight) volume else NumberUtils.divideWithMaxScale(volume, price)
    }

    private fun getVolume(volume: BigDecimal, straight: Boolean, price: BigDecimal): BigDecimal {
        return if (straight) volume else volume * price
    }

    private fun getBalance(order: Order): BigDecimal {
        val assetPair = assetsPairsHolder.getAssetPair(order.assetPairId)
        val asset = if (order.isBuySide()) assetPair.quotingAssetId else assetPair.baseAssetId
        return balancesHolder.getAvailableBalance(order.clientId, asset)
    }

    private fun getMarketBalance(availableBalances: MutableMap<String, MutableMap<String, BigDecimal>>, order: Order, asset: Asset): BigDecimal {
        return availableBalances.getOrPut(order.clientId) { HashMap() }[asset.assetId]!!
    }

    private fun setMarketBalance(availableBalances: MutableMap<String, MutableMap<String, BigDecimal>>, order: Order, asset: Asset, value: BigDecimal) {
        availableBalances.getOrPut(order.clientId) { HashMap() }[asset.assetId] = value
    }

    private fun calculateExecutionPrice(order: Order,
                                        assetPair: AssetPair,
                                        totalLimitPrice: BigDecimal,
                                        totalVolume: BigDecimal): BigDecimal {
        return NumberUtils.setScale(if (order.isStraight())
            NumberUtils.divideWithMaxScale(totalLimitPrice, order.getAbsVolume())
        else
            NumberUtils.divideWithMaxScale(order.getAbsVolume(), totalVolume),
                assetPair.accuracy,
                order.isOrigBuySide())
    }

    private fun checkMaxVolume(order: Order,
                               assetPair: AssetPair,
                               executionPrice: BigDecimal): Boolean {
        return when {
            order.takePrice() != null || assetPair.maxVolume == null -> true
            order.isStraight() -> order.getAbsVolume() <= assetPair.maxVolume
            else -> order.getAbsVolume() / executionPrice <= assetPair.maxVolume
        }
    }

    private fun checkMaxValue(order: Order,
                              assetPair: AssetPair,
                              executionPrice: BigDecimal): Boolean {
        return when {
            order.takePrice() != null || assetPair.maxValue == null -> true
            order.isStraight() -> order.getAbsVolume() * executionPrice <= assetPair.maxValue
            else -> order.getAbsVolume() <= assetPair.maxValue
        }
    }

    private fun checkExecutionPriceDeviation(isBuySide: Boolean,
                                             price: BigDecimal,
                                             expectedPrice: BigDecimal?,
                                             threshold: BigDecimal?): Boolean {
        if (threshold == null || expectedPrice == null) {
            return true
        }
        if (NumberUtils.equalsIgnoreScale(BigDecimal.ZERO, expectedPrice)) {
            return false
        }
        return if (isBuySide) {
            NumberUtils.divideWithMaxScale(price - expectedPrice, expectedPrice) <= threshold
        } else {
            NumberUtils.divideWithMaxScale(expectedPrice - price, expectedPrice) <= threshold
        }
    }
}
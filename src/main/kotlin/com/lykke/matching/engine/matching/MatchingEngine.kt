package com.lykke.matching.engine.matching

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.CopyWrapper
import com.lykke.matching.engine.daos.LkkTrade
import com.lykke.matching.engine.daos.NewLimitOrder
import com.lykke.matching.engine.daos.NewOrder
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
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.utils.RoundingUtils
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
                     private val balancesHolder: BalancesHolder) {

    companion object {
        private const val RELATIVE_SPREAD_ACCURACY = 4
    }

    private val feeProcessor = FeeProcessor(balancesHolder, assetsHolder, assetsPairsHolder, genericLimitOrderService)
    private var tradeIndex: Long = 0

    fun initTransaction(): MatchingEngine {
        tradeIndex = 0
        return this
    }

    fun match(originOrder: NewOrder, orderBook: PriorityBlockingQueue<NewLimitOrder>, balance: BigDecimal? = null): MatchingResult {
        val orderWrapper = CopyWrapper(originOrder)
        val order = orderWrapper.copy
        val availableBalance = balance ?: getBalance(order)
        val workingOrderBook = PriorityBlockingQueue(orderBook)
        var remainingVolume = order.getAbsVolume()
        val matchedOrders = LinkedList<CopyWrapper<NewLimitOrder>>()
        val skipLimitOrders = HashSet<NewLimitOrder>()
        val cancelledLimitOrders = HashSet<NewLimitOrder>()
        var totalLimitPrice = BigDecimal.ZERO
        var totalVolume = BigDecimal.ZERO
        val limitReservedBalances = HashMap<String, BigDecimal>() // limit reserved balances for trades funds control
        val availableBalances = HashMap<String, MutableMap<String, BigDecimal>>() // clientId -> assetId -> balance; available balances for market balance control and fee funds control
        val now = Date()
        val assetPair = assetsPairsHolder.getAssetPair(order.assetPairId)
        val isBuy = order.isBuySide()
        val lkkTrades = LinkedList<LkkTrade>()
        val completedLimitOrders = LinkedList<NewLimitOrder>()
        var uncompletedLimitOrder: CopyWrapper<NewLimitOrder>? = null
        val allOwnCashMovements = LinkedList<WalletOperation>()
        val allOppositeCashMovements = LinkedList<WalletOperation>()
        val asset = assetsHolder.getAsset(if (isBuy) assetPair.quotingAssetId else assetPair.baseAssetId)
        val limitAsset = assetsHolder.getAsset(if (isBuy) assetPair.baseAssetId else assetPair.quotingAssetId)

        setMarketBalance(availableBalances, order, asset, availableBalance)

        val marketOrderTrades = LinkedList<TradeInfo>()

        val limitOrdersReport = LimitOrdersReport()
        var totalLimitVolume = BigDecimal.ZERO

        if (checkOrderBook(order, workingOrderBook)) {
            while (getMarketBalance(availableBalances, order, asset) >= BigDecimal.ZERO && workingOrderBook.size > 0 && !RoundingUtils.equalsWithDefaultDelta(remainingVolume, BigDecimal.ZERO)
                    && (order.takePrice() == null || (if (isBuy) order.takePrice()!! >= workingOrderBook.peek().price else order.takePrice()!! <= workingOrderBook.peek().price))) {
                val limitOrder = workingOrderBook.poll()
                if (order.clientId == limitOrder.clientId) {
                    skipLimitOrders.add(limitOrder)
                    continue
                }

                var isFullyMatched = false

                val limitRemainingVolume = limitOrder.getAbsRemainingVolume()
                val marketRemainingVolume = getCrossVolume(remainingVolume, order.isStraight(), limitOrder.price)
                val volume = if (marketRemainingVolume > limitRemainingVolume) limitRemainingVolume else { isFullyMatched = true; marketRemainingVolume}


                var marketRoundedVolume = RoundingUtils.setScale(if (isBuy) volume else -volume, assetsHolder.getAsset(assetPair.baseAssetId).accuracy, !isBuy)
                var oppositeRoundedVolume = RoundingUtils.setScale(if (isBuy) -limitOrder.price * volume else limitOrder.price * volume, assetsHolder.getAsset(assetPair.quotingAssetId).accuracy, isBuy)

                LOGGER.info("Matching with limit order ${limitOrder.externalId}, client ${limitOrder.clientId}, price ${limitOrder.price}, " +
                        "marketVolume ${RoundingUtils.roundForPrint(if (isBuy) oppositeRoundedVolume else marketRoundedVolume)}, " +
                        "limitVolume ${RoundingUtils.roundForPrint(if (isBuy) marketRoundedVolume else oppositeRoundedVolume)}")

                if ((!order.isStraight()) && isFullyMatched) {
                    oppositeRoundedVolume = BigDecimal.valueOf(order.volume.signum().toLong()) * (RoundingUtils.setScale(order.volume.abs() - totalLimitVolume.abs(), assetsHolder.getAsset(assetPair.quotingAssetId).accuracy, isBuy))
                    marketRoundedVolume = RoundingUtils.setScale( RoundingUtils.divideWithMaxScale(-oppositeRoundedVolume, limitOrder.price), assetsHolder.getAsset(assetPair.baseAssetId).accuracy, !isBuy)
                    LOGGER.info("Rounding last matched limit order trade: ${RoundingUtils.roundForPrint(marketRoundedVolume)}")
                }

                LOGGER.info("Corrected volumes: " +
                        "marketVolume ${RoundingUtils.roundForPrint(if (isBuy) oppositeRoundedVolume else marketRoundedVolume)}, " +
                        "limitVolume ${RoundingUtils.roundForPrint(if (isBuy) marketRoundedVolume else oppositeRoundedVolume)}")

                if (!genericLimitOrderService.checkAndReduceBalance(
                        limitOrder,
                        if (isBuy) marketRoundedVolume else oppositeRoundedVolume,
                        limitReservedBalances)) {
                    LOGGER.info("Added order (id: ${limitOrder.externalId}, client: ${limitOrder.clientId}, asset: ${limitOrder.assetPairId}) to cancelled limit orders")
                    cancelledLimitOrders.add(limitOrder)
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
                val relativeSpread = if (validSpread) RoundingUtils.divideWithMaxScale(absoluteSpread!!, bestAsk) else null

                val makerFees = try {
                    feeProcessor.processMakerFee(limitOrder.fees ?: emptyList(),
                            if (isBuy) limitQuotingAssetOperation else limitBaseAssetOperation,
                            oppositeCashMovements,
                            relativeSpread,
                            mapOf(Pair(assetPair.assetPairId, limitOrder.price)),
                            availableBalances)
                } catch (e: FeeException) {
                    LOGGER.info("Added order (id: ${limitOrder.externalId}, client: ${limitOrder.clientId}, asset: ${limitOrder.assetPairId}) to cancelled limit orders: ${e.message}")
                    cancelledLimitOrders.add(limitOrder)
                    continue
                }

                val takerFees = try {
                    feeProcessor.processFee(order.fees ?: emptyList(), if (isBuy) baseAssetOperation else quotingAssetOperation, ownCashMovements, mapOf(Pair(assetPair.assetPairId, limitOrder.price)), availableBalances)
                } catch (e: NotEnoughFundsFeeException) {
                    order.status = OrderStatus.NotEnoughFunds.name
                    LOGGER.info("Not enough funds for fee for order id: ${order.externalId}, client: ${order.clientId}, asset: ${order.assetPairId}, volume: ${RoundingUtils.roundForPrint(order.volume)}, price: ${order.takePrice()}, marketBalance: ${getMarketBalance(availableBalances, order, asset)} : ${e.message}")
                    return MatchingResult(orderWrapper, now, cancelledLimitOrders)
                } catch (e: FeeException) {
                    order.status = OrderStatus.InvalidFee.name
                    LOGGER.info("Invalid fee for order id: ${order.externalId}, client: ${order.clientId}, asset: ${order.assetPairId}, volume: ${RoundingUtils.roundForPrint(order.volume)}, price: ${order.takePrice()}, marketBalance: ${getMarketBalance(availableBalances, order, asset)} : ${e.message}")
                    return MatchingResult(orderWrapper, now, cancelledLimitOrders)
                }
                if (takerFees.isNotEmpty()) {
                    LOGGER.info("Taker fee transfers: ${takerFees.map { it.transfer }}")
                }
                if (makerFees.isNotEmpty()) {
                    LOGGER.info("Maker fee transfers: ${makerFees.map { it.transfer }}")
                }

                val limitOrderCopyWrapper = CopyWrapper(limitOrder)
                val limitOrderCopy = limitOrderCopyWrapper.copy
                if (limitOrderCopy.reservedLimitVolume != null && limitOrderCopy.reservedLimitVolume!! > BigDecimal.ZERO) {
                    limitOrderCopy.reservedLimitVolume =  RoundingUtils.setScaleRoundHalfUp(limitOrderCopy.reservedLimitVolume!! + if (-marketRoundedVolume < BigDecimal.ZERO) -marketRoundedVolume else -oppositeRoundedVolume, limitAsset.accuracy)
                }

                val limitVolumeAsset = assetsHolder.getAsset(assetsPairsHolder.getAssetPair(limitOrder.assetPairId).baseAssetId)
                val newRemainingVolume = RoundingUtils.setScaleRoundHalfUp(limitOrderCopy.remainingVolume + marketRoundedVolume, limitVolumeAsset.accuracy)
                val isLimitMatched = newRemainingVolume.signum() != limitOrderCopy.remainingVolume.signum()
                if (isLimitMatched) {
                    if (newRemainingVolume.signum() * limitOrderCopy.remainingVolume.signum() < 0) {
                        LOGGER.info("Matched volume is overflowed (previous: ${limitOrderCopy.remainingVolume}, current: $newRemainingVolume)")
                    }
                    lkkTrades.add(LkkTrade(limitOrder.assetPairId, limitOrder.clientId, limitOrder.price, limitOrderCopy.remainingVolume, now))
                    lkkTrades.add(LkkTrade(limitOrder.assetPairId, order.clientId, limitOrder.price, -limitOrderCopy.remainingVolume, now))
                    limitOrderCopy.remainingVolume = BigDecimal.ZERO
                    limitOrderCopy.status = OrderStatus.Matched.name
                    completedLimitOrders.add(limitOrder)
                    if (limitOrderCopy.reservedLimitVolume != null && limitOrderCopy.reservedLimitVolume!! > BigDecimal.ZERO) {
                        oppositeCashMovements.add(WalletOperation(UUID.randomUUID().toString(), null, limitOrder.clientId, if (-marketRoundedVolume < BigDecimal.ZERO) assetPair.baseAssetId else assetPair.quotingAssetId, now, BigDecimal.ZERO, -limitOrderCopy.reservedLimitVolume!!))
                        limitOrderCopy.reservedLimitVolume =  BigDecimal.ZERO
                    }
                } else {
                    lkkTrades.add(LkkTrade(limitOrder.assetPairId, limitOrder.clientId, limitOrder.price, -marketRoundedVolume, now))
                    lkkTrades.add(LkkTrade(limitOrder.assetPairId, order.clientId, limitOrder.price, marketRoundedVolume, now))
                    limitOrderCopy.remainingVolume = newRemainingVolume
                    limitOrderCopy.status = OrderStatus.Processing.name
                    uncompletedLimitOrder = limitOrderCopyWrapper
                }

                setMarketBalance(availableBalances, order, asset, RoundingUtils.setScaleRoundHalfUp(getMarketBalance(availableBalances, order, asset) - (if (isBuy) oppositeRoundedVolume else marketRoundedVolume).abs() /* - assetFeeAmount*/, asset.accuracy))

                remainingVolume = if (isFullyMatched) BigDecimal.ZERO else RoundingUtils.setScale(remainingVolume - getVolume(marketRoundedVolume.abs(), order.isStraight(), limitOrder.price), assetsHolder.getAsset(if (order.isStraight()) assetPair.baseAssetId else assetPair.quotingAssetId).accuracy, order.isOrigBuySide())
                limitOrderCopy.lastMatchTime = now

                allOppositeCashMovements.addAll(oppositeCashMovements)
                allOwnCashMovements.addAll(ownCashMovements)
                val traderId = UUID.randomUUID().toString()

                val roundedAbsoluteSpread = if (absoluteSpread != null) RoundingUtils.setScaleRoundHalfUp(absoluteSpread, assetPair.accuracy) else null
                val roundedRelativeSpread = if (relativeSpread != null) RoundingUtils.setScaleRoundHalfUp(relativeSpread, RELATIVE_SPREAD_ACCURACY) else null

                marketOrderTrades.add(TradeInfo(traderId,
                        order.clientId,
                        RoundingUtils.setScaleRoundHalfUp((if (isBuy) oppositeRoundedVolume else marketRoundedVolume).abs(), asset.accuracy).toPlainString(),
                        asset.assetId,
                        limitOrder.clientId,
                        RoundingUtils.setScaleRoundHalfUp((if (isBuy) marketRoundedVolume else oppositeRoundedVolume).abs(), limitAsset.accuracy).toPlainString(),
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
                        roundedRelativeSpread))

                limitOrdersReport.orders.add(LimitOrderWithTrades(limitOrder,
                        mutableListOf(LimitTradeInfo(traderId,
                                limitOrder.clientId,
                                limitAsset.assetId,
                                RoundingUtils.setScaleRoundHalfUp((if (isBuy) marketRoundedVolume else oppositeRoundedVolume).abs(), limitAsset.accuracy).toPlainString(),
                                limitOrder.price,
                                now,
                                order.id,
                                order.externalId,
                                asset.assetId,
                                order.clientId,
                                RoundingUtils.setScaleRoundHalfUp((if (isBuy) oppositeRoundedVolume else marketRoundedVolume).abs(), asset.accuracy).toPlainString(),
                                tradeIndex,
                                limitOrder.fee,
                                singleFeeTransfer(limitOrder.fee, makerFees),
                                makerFees,
                                roundedAbsoluteSpread,
                                roundedRelativeSpread))))
                tradeIndex++

                totalVolume += volume
                totalLimitPrice += volume * limitOrder.price
                totalLimitVolume += (if (order.isStraight()) marketRoundedVolume else oppositeRoundedVolume).abs()
                matchedOrders.add(limitOrderCopyWrapper)
            }
        }

        if (order.takePrice() == null && remainingVolume > BigDecimal.ZERO) {
            order.status = OrderStatus.NoLiquidity.name
            LOGGER.info("No liquidity, not enough funds on limit orders, for market order id: ${order.externalId}}, client: ${order.clientId}, asset: ${order.assetPairId}, volume: ${RoundingUtils.roundForPrint(order.volume)} | Unfilled: ${RoundingUtils.roundForPrint(remainingVolume)}, price: ${order.takePrice()}")
            return MatchingResult(orderWrapper, now, cancelledLimitOrders)
        }

        if (order.calculateReservedVolume() > availableBalance) {
            order.status = OrderStatus.ReservedVolumeGreaterThanBalance.name
            LOGGER.info("Reserved volume (${order.calculateReservedVolume()}) greater than balance ($availableBalance) for order id: ${order.externalId}, client: ${order.clientId}, asset: ${order.assetPairId}, volume: ${RoundingUtils.roundForPrint(order.volume)}, price: ${order.takePrice()}")
            return MatchingResult(orderWrapper, now, cancelledLimitOrders)
        }

        val reservedBalance = if (order.calculateReservedVolume() > BigDecimal.ZERO)  RoundingUtils.setScale(order.calculateReservedVolume(), asset.accuracy, true) else availableBalance
        val marketBalance = getMarketBalance(availableBalances, order, asset)
        if (marketBalance < BigDecimal.ZERO || reservedBalance < RoundingUtils.setScale((if (isBuy) totalLimitPrice else totalVolume), asset.accuracy, true)) {
            order.status = OrderStatus.NotEnoughFunds.name
            LOGGER.info("Not enough funds for order id: ${order.externalId}, client: ${order.clientId}, asset: ${order.assetPairId}, volume: ${RoundingUtils.roundForPrint(order.volume)}, price: ${order.takePrice()}, marketBalance: $marketBalance : $reservedBalance < ${RoundingUtils.round((if(isBuy) totalLimitPrice else totalVolume).toDouble(), asset.accuracy, true)}")
            return MatchingResult(orderWrapper, now, cancelledLimitOrders)
        }

        if (order.takePrice() != null && remainingVolume > BigDecimal.ZERO) {
            order.status = OrderStatus.Processing.name
            order.updateRemainingVolume(if (order.isBuySide() ||  RoundingUtils.equalsIgnoreScale(remainingVolume, BigDecimal.ZERO) ) remainingVolume else -remainingVolume)
        } else {
            order.status = OrderStatus.Matched.name
            order.updateRemainingVolume(BigDecimal.ZERO)
        }
        order.updateMatchTime(now)
        order.updatePrice(RoundingUtils.setScale(
                if (order.isStraight()) RoundingUtils.divideWithMaxScale(totalLimitPrice, order.getAbsVolume())
                else RoundingUtils.divideWithMaxScale(order.getAbsVolume(), totalVolume),
                assetsPairsHolder.getAssetPair(order.assetPairId).accuracy, order.isOrigBuySide()))

        return MatchingResult(orderWrapper,
                now,
                cancelledLimitOrders,
                matchedOrders,
                skipLimitOrders,
                completedLimitOrders,
                uncompletedLimitOrder,
                lkkTrades,
                allOwnCashMovements,
                allOppositeCashMovements,
                marketOrderTrades,
                limitOrdersReport,
                workingOrderBook,
                marketBalance,
                false)
    }

    private fun checkOrderBook(order: NewOrder, orderBook: PriorityBlockingQueue<NewLimitOrder>): Boolean =
            orderBook.isEmpty() || orderBook.peek().assetPairId == order.assetPairId && orderBook.peek().isBuySide() != order.isBuySide()

    private fun getCrossVolume(volume: BigDecimal, straight: Boolean, price: BigDecimal): BigDecimal {
        return if (straight) volume else RoundingUtils.divideWithMaxScale(volume, price)
    }

    private fun getVolume(volume: BigDecimal, straight: Boolean, price: BigDecimal): BigDecimal {
        return if (straight) volume else volume * price
    }

    private fun getBalance(order: NewOrder): BigDecimal {
        val assetPair = assetsPairsHolder.getAssetPair(order.assetPairId)
        val asset = if (order.isBuySide()) assetPair.quotingAssetId else assetPair.baseAssetId
        return balancesHolder.getAvailableBalance(order.clientId, asset)
    }

    private fun getMarketBalance(availableBalances: MutableMap<String, MutableMap<String, BigDecimal>>, order: NewOrder, asset: Asset): BigDecimal {
        return availableBalances.getOrPut(order.clientId) { HashMap() }[asset.assetId]!!
    }

    private fun setMarketBalance(availableBalances: MutableMap<String, MutableMap<String, BigDecimal>>, order: NewOrder, asset: Asset, value: BigDecimal) {
        availableBalances.getOrPut(order.clientId) { HashMap() }[asset.assetId] = value
    }
}
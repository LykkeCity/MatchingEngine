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
import com.lykke.matching.engine.greaterThan
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.outgoing.messages.LimitOrderWithTrades
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.outgoing.messages.LimitTradeInfo
import com.lykke.matching.engine.outgoing.messages.TradeInfo
import com.lykke.matching.engine.round
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

    fun match(originOrder: NewOrder, orderBook: PriorityBlockingQueue<NewLimitOrder>, balance: Double? = null): MatchingResult {
        val orderWrapper = CopyWrapper(originOrder)
        val order = orderWrapper.copy
        val availableBalance = balance ?: getBalance(order)
        val workingOrderBook = PriorityBlockingQueue(orderBook)
        var remainingVolume = order.getAbsVolume()
        val matchedOrders = LinkedList<CopyWrapper<NewLimitOrder>>()
        val skipLimitOrders = HashSet<NewLimitOrder>()
        val cancelledLimitOrders = HashSet<NewLimitOrder>()
        var totalLimitPrice = BigDecimal.valueOf(0.0)
        var totalVolume = BigDecimal.valueOf(0.0)
        val limitReservedBalances = HashMap<String, Double>() // limit reserved balances for trades funds control
        val availableBalances = HashMap<String, MutableMap<String, Double>>() // clientId -> assetId -> balance; available balances for market balance control and fee funds control
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
        var totalLimitVolume = BigDecimal.valueOf(0.0)

        if (checkOrderBook(order, workingOrderBook)) {
            while (getMarketBalance(availableBalances, order, asset) >= 0 && workingOrderBook.size > 0 && remainingVolume.greaterThan(0.0) && (order.takePrice() == null || (if (isBuy) order.takePrice()!! >= workingOrderBook.peek().price else order.takePrice()!! <= workingOrderBook.peek().price))) {
                val limitOrder = workingOrderBook.poll()
                if (order.clientId == limitOrder.clientId) {
                    skipLimitOrders.add(limitOrder)
                    continue
                }

                var isFullyMatched = false

                val limitRemainingVolume = limitOrder.getAbsRemainingVolume()
                val marketRemainingVolume = getCrossVolume(remainingVolume, order.isStraight(), limitOrder.price)
                val volume = if (marketRemainingVolume > limitRemainingVolume) limitRemainingVolume else { isFullyMatched = true; marketRemainingVolume}

                var marketRoundedVolume = NumberUtils.round(if (isBuy) volume else -volume, assetsHolder.getAsset(assetPair.baseAssetId).accuracy, !isBuy)
                var oppositeRoundedVolume = NumberUtils.round(if (isBuy) -limitOrder.price * volume else limitOrder.price * volume, assetsHolder.getAsset(assetPair.quotingAssetId).accuracy, isBuy)

                LOGGER.info("Matching with limit order ${limitOrder.externalId}, client ${limitOrder.clientId}, price ${limitOrder.price}, " +
                        "marketVolume ${NumberUtils.roundForPrint(if (isBuy) oppositeRoundedVolume else marketRoundedVolume)}, " +
                        "limitVolume ${NumberUtils.roundForPrint(if (isBuy) marketRoundedVolume else oppositeRoundedVolume)}")

                if ((!order.isStraight()) && isFullyMatched) {
                    oppositeRoundedVolume = Math.signum(order.volume) * (NumberUtils.round(Math.abs(order.volume) - Math.abs(totalLimitVolume.toDouble()), assetsHolder.getAsset(assetPair.quotingAssetId).accuracy, isBuy))
                    marketRoundedVolume = NumberUtils.round(-oppositeRoundedVolume / limitOrder.price, assetsHolder.getAsset(assetPair.baseAssetId).accuracy, !isBuy)
                    LOGGER.info("Rounding last matched limit order trade: ${NumberUtils.roundForPrint(marketRoundedVolume)}")
                }

                LOGGER.info("Corrected volumes: " +
                        "marketVolume ${NumberUtils.roundForPrint(if (isBuy) oppositeRoundedVolume else marketRoundedVolume)}, " +
                        "limitVolume ${NumberUtils.roundForPrint(if (isBuy) marketRoundedVolume else oppositeRoundedVolume)}")

                if (!genericLimitOrderService.checkAndReduceBalance(
                        limitOrder,
                        if (isBuy) marketRoundedVolume else oppositeRoundedVolume,
                        limitReservedBalances)) {
                    LOGGER.info("Added order (id: ${limitOrder.externalId}, client: ${limitOrder.clientId}, asset: ${limitOrder.assetPairId}) to cancelled limit orders")
                    cancelledLimitOrders.add(limitOrder)
                    continue
                }

                val baseAssetOperation = WalletOperation(UUID.randomUUID().toString(), null, order.clientId, assetPair.baseAssetId, now, marketRoundedVolume, 0.0)
                val quotingAssetOperation = WalletOperation(UUID.randomUUID().toString(), null, order.clientId, assetPair.quotingAssetId, now, oppositeRoundedVolume, 0.0)
                val limitBaseAssetOperation = WalletOperation(UUID.randomUUID().toString(), null, limitOrder.clientId, assetPair.baseAssetId, now, -marketRoundedVolume, if (-marketRoundedVolume < 0) -marketRoundedVolume else 0.0)
                val limitQuotingAssetOperation = WalletOperation(UUID.randomUUID().toString(), null, limitOrder.clientId, assetPair.quotingAssetId, now, -oppositeRoundedVolume, if (-oppositeRoundedVolume < 0) -oppositeRoundedVolume else 0.0)

                val ownCashMovements = mutableListOf(baseAssetOperation, quotingAssetOperation)
                val oppositeCashMovements = mutableListOf(limitBaseAssetOperation, limitQuotingAssetOperation)

                val bestAsk = if (isBuy) limitOrder.price else genericLimitOrderService.getOrderBook(limitOrder.assetPairId).getAskPrice()
                val bestBid = if (isBuy) genericLimitOrderService.getOrderBook(limitOrder.assetPairId).getBidPrice() else limitOrder.price
                val validSpread = bestAsk > 0.0 && bestBid > 0.0
                val absoluteSpread = if (validSpread) bestAsk - bestBid else null
                val relativeSpread = if (validSpread) absoluteSpread!! / bestAsk else null

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
                    LOGGER.info("Not enough funds for fee for order id: ${order.externalId}, client: ${order.clientId}, asset: ${order.assetPairId}, volume: ${NumberUtils.roundForPrint(order.volume)}, price: ${order.takePrice()}, marketBalance: ${getMarketBalance(availableBalances, order, asset)} : ${e.message}")
                    return MatchingResult(orderWrapper, now, cancelledLimitOrders)
                } catch (e: FeeException) {
                    order.status = OrderStatus.InvalidFee.name
                    LOGGER.info("Invalid fee for order id: ${order.externalId}, client: ${order.clientId}, asset: ${order.assetPairId}, volume: ${NumberUtils.roundForPrint(order.volume)}, price: ${order.takePrice()}, marketBalance: ${getMarketBalance(availableBalances, order, asset)} : ${e.message}")
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
                if (limitOrderCopy.reservedLimitVolume != null && limitOrderCopy.reservedLimitVolume!! > 0) {
                    limitOrderCopy.reservedLimitVolume =  NumberUtils.parseDouble(limitOrderCopy.reservedLimitVolume!! + if (-marketRoundedVolume < 0) -marketRoundedVolume else -oppositeRoundedVolume, limitAsset.accuracy).toDouble()
                }

                val limitVolumeAsset = assetsHolder.getAsset(assetsPairsHolder.getAssetPair(limitOrder.assetPairId).baseAssetId)
                val newRemainingVolume = NumberUtils.parseDouble(limitOrderCopy.remainingVolume + marketRoundedVolume, limitVolumeAsset.accuracy).toDouble()
                val isLimitMatched = Math.signum(newRemainingVolume) != Math.signum(limitOrderCopy.remainingVolume)
                if (isLimitMatched) {
                    if (Math.signum(newRemainingVolume) * Math.signum(limitOrderCopy.remainingVolume) < 0) {
                        LOGGER.info("Matched volume is overflowed (previous: ${limitOrderCopy.remainingVolume}, current: $newRemainingVolume)")
                    }
                    lkkTrades.add(LkkTrade(limitOrder.assetPairId, limitOrder.clientId, limitOrder.price, limitOrderCopy.remainingVolume, now))
                    lkkTrades.add(LkkTrade(limitOrder.assetPairId, order.clientId, limitOrder.price, -limitOrderCopy.remainingVolume, now))
                    limitOrderCopy.remainingVolume = 0.0
                    limitOrderCopy.status = OrderStatus.Matched.name
                    completedLimitOrders.add(limitOrder)
                    if (limitOrderCopy.reservedLimitVolume != null && limitOrderCopy.reservedLimitVolume!! > 0) {
                        oppositeCashMovements.add(WalletOperation(UUID.randomUUID().toString(), null, limitOrder.clientId, if (-marketRoundedVolume < 0) assetPair.baseAssetId else assetPair.quotingAssetId, now, 0.0, -limitOrderCopy.reservedLimitVolume!!))
                        limitOrderCopy.reservedLimitVolume =  0.0
                    }
                } else {
                    lkkTrades.add(LkkTrade(limitOrder.assetPairId, limitOrder.clientId, limitOrder.price, -marketRoundedVolume, now))
                    lkkTrades.add(LkkTrade(limitOrder.assetPairId, order.clientId, limitOrder.price, marketRoundedVolume, now))
                    limitOrderCopy.remainingVolume = newRemainingVolume
                    limitOrderCopy.status = OrderStatus.Processing.name
                    uncompletedLimitOrder = limitOrderCopyWrapper
                }

                setMarketBalance(availableBalances, order, asset, NumberUtils.parseDouble(getMarketBalance(availableBalances, order, asset) - Math.abs(if (isBuy) oppositeRoundedVolume else marketRoundedVolume)/* - assetFeeAmount*/, asset.accuracy).toDouble())

                remainingVolume = if (isFullyMatched) 0.0 else NumberUtils.round(remainingVolume - getVolume(Math.abs(marketRoundedVolume), order.isStraight(), limitOrder.price), assetsHolder.getAsset(if (order.isStraight()) assetPair.baseAssetId else assetPair.quotingAssetId).accuracy, order.isOrigBuySide())
                limitOrderCopy.lastMatchTime = now

                allOppositeCashMovements.addAll(oppositeCashMovements)
                allOwnCashMovements.addAll(ownCashMovements)
                val traderId = UUID.randomUUID().toString()

                val roundedAbsoluteSpread = if (absoluteSpread != null) NumberUtils.parseDouble(absoluteSpread, assetPair.accuracy).toDouble() else null
                val roundedRelativeSpread = if (relativeSpread != null) NumberUtils.parseDouble(relativeSpread, RELATIVE_SPREAD_ACCURACY).toDouble() else null

                marketOrderTrades.add(TradeInfo(traderId,
                        order.clientId,
                        Math.abs(if (isBuy) oppositeRoundedVolume else marketRoundedVolume).round(asset.accuracy),
                        asset.assetId,
                        limitOrder.clientId,
                        Math.abs(if (isBuy) marketRoundedVolume else oppositeRoundedVolume).round(limitAsset.accuracy),
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
                                Math.abs(if (isBuy) marketRoundedVolume else oppositeRoundedVolume).round(limitAsset.accuracy),
                                limitOrder.price,
                                now,
                                order.id,
                                order.externalId,
                                asset.assetId,
                                order.clientId,
                                Math.abs(if (isBuy) oppositeRoundedVolume else marketRoundedVolume).round(asset.accuracy),
                                tradeIndex,
                                limitOrder.fee,
                                singleFeeTransfer(limitOrder.fee, makerFees),
                                makerFees,
                                roundedAbsoluteSpread,
                                roundedRelativeSpread))))
                tradeIndex++

                totalVolume += BigDecimal.valueOf(volume)
                totalLimitPrice += BigDecimal.valueOf(volume) * BigDecimal.valueOf(limitOrder.price)
                totalLimitVolume += BigDecimal.valueOf(Math.abs(if (order.isStraight()) marketRoundedVolume else oppositeRoundedVolume))
                matchedOrders.add(limitOrderCopyWrapper)
            }
        }

        if (order.takePrice() == null && remainingVolume > 0) {
            order.status = OrderStatus.NoLiquidity.name
            LOGGER.info("No liquidity, not enough funds on limit orders, for market order id: ${order.externalId}}, client: ${order.clientId}, asset: ${order.assetPairId}, volume: ${NumberUtils.roundForPrint(order.volume)} | Unfilled: ${NumberUtils.roundForPrint(remainingVolume)}, price: ${order.takePrice()}")
            return MatchingResult(orderWrapper, now, cancelledLimitOrders)
        }

        if (order.calculateReservedVolume() > availableBalance) {
            order.status = OrderStatus.ReservedVolumeGreaterThanBalance.name
            LOGGER.info("Reserved volume (${order.calculateReservedVolume()}) greater than balance ($availableBalance) for order id: ${order.externalId}, client: ${order.clientId}, asset: ${order.assetPairId}, volume: ${NumberUtils.roundForPrint(order.volume)}, price: ${order.takePrice()}")
            return MatchingResult(orderWrapper, now, cancelledLimitOrders)
        }

        val reservedBalance = if (order.calculateReservedVolume() > 0.0)  NumberUtils.round(order.calculateReservedVolume(), asset.accuracy, true) else availableBalance
        val marketBalance = getMarketBalance(availableBalances, order, asset)
        if (marketBalance < 0 || reservedBalance < NumberUtils.round((if (isBuy) totalLimitPrice else totalVolume).toDouble(), asset.accuracy, true)) {
            order.status = OrderStatus.NotEnoughFunds.name
            LOGGER.info("Not enough funds for order id: ${order.externalId}, client: ${order.clientId}, asset: ${order.assetPairId}, volume: ${NumberUtils.roundForPrint(order.volume)}, price: ${order.takePrice()}, marketBalance: $marketBalance : $reservedBalance < ${NumberUtils.round((if(isBuy) totalLimitPrice else totalVolume).toDouble(), asset.accuracy, true)}")
            return MatchingResult(orderWrapper, now, cancelledLimitOrders)
        }

        if (order.takePrice() != null && remainingVolume > 0.0) {
            order.status = OrderStatus.Processing.name
            order.updateRemainingVolume(if (order.isBuySide() || remainingVolume == 0.0) remainingVolume else -remainingVolume)
        } else {
            order.status = OrderStatus.Matched.name
            order.updateRemainingVolume(0.0)
        }
        order.updateMatchTime(now)
        order.updatePrice(NumberUtils.round(if (order.isStraight()) totalLimitPrice.toDouble() / order.getAbsVolume() else order.getAbsVolume() / totalVolume.toDouble()
                , assetsPairsHolder.getAssetPair(order.assetPairId).accuracy, order.isOrigBuySide()))

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

    private fun getCrossVolume(volume: Double, straight: Boolean, price: Double): Double {
        return if (straight) volume else volume / price
    }

    private fun getVolume(volume: Double, straight: Boolean, price: Double): Double {
        return if (straight) volume else volume * price
    }

    private fun getBalance(order: NewOrder): Double {
        val assetPair = assetsPairsHolder.getAssetPair(order.assetPairId)
        val asset = if (order.isBuySide()) assetPair.quotingAssetId else assetPair.baseAssetId
        return balancesHolder.getAvailableBalance(order.clientId, asset)
    }

    private fun getMarketBalance(availableBalances: MutableMap<String, MutableMap<String, Double>>, order: NewOrder, asset: Asset): Double {
        return availableBalances.getOrPut(order.clientId) { HashMap() }[asset.assetId]!!
    }

    private fun setMarketBalance(availableBalances: MutableMap<String, MutableMap<String, Double>>, order: NewOrder, asset: Asset, value: Double) {
        availableBalances.getOrPut(order.clientId) { HashMap() }[asset.assetId] = value
    }
}
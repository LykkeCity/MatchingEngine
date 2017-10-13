package com.lykke.matching.engine.matching

import com.lykke.matching.engine.daos.LkkTrade
import com.lykke.matching.engine.daos.NewLimitOrder
import com.lykke.matching.engine.daos.NewOrder
import com.lykke.matching.engine.daos.WalletOperation
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
import com.lykke.matching.engine.utils.RoundingUtils
import org.apache.log4j.Logger
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

    fun match(order: NewOrder, orderBook: PriorityBlockingQueue<NewLimitOrder>, balance: Double? = null): MatchingResult {
        val availableBalance = balance ?: getBalance(order)
        val workingOrderBook = PriorityBlockingQueue(orderBook)
        var remainingVolume = order.getAbsVolume()
        val matchedOrders = LinkedList<NewLimitOrder>()
        val skipLimitOrders = HashSet<NewLimitOrder>()
        val cancelledLimitOrders = HashSet<NewLimitOrder>()

        var totalLimitPrice = 0.0
        var totalVolume = 0.0
        val limitBalances = HashMap<String, Double>()

        val assetPair = assetsPairsHolder.getAssetPair(order.assetPairId)

        while (workingOrderBook.size > 0 && ((order.takePrice() == null && remainingVolume.greaterThan(0.0))
                || (order.takePrice() != null && (if (order.isBuySide()) order.takePrice()!! >= workingOrderBook.peek().price else order.takePrice()!! <= workingOrderBook.peek().price) && remainingVolume.greaterThan(0.0)))
                ) {
            val limitOrder = workingOrderBook.poll()
            val limitRemainingVolume = limitOrder.getAbsRemainingVolume()
            val marketRemainingVolume = getCrossVolume(remainingVolume, order.isStraight(), limitOrder.price)
            val volume = if (marketRemainingVolume >= limitRemainingVolume) limitRemainingVolume else marketRemainingVolume
            val limitAsset = if (limitOrder.isBuySide()) assetPair.quotingAssetId else assetPair.baseAssetId
            val limitBalance = limitBalances[limitOrder.clientId] ?: balancesHolder.getAvailableReservedBalance(limitOrder.clientId, if (limitOrder.isBuySide()) assetPair.quotingAssetId else assetPair.baseAssetId)
            val limitVolume = RoundingUtils.round(Math.abs(if (limitOrder.isBuySide()) volume * limitOrder.price else volume), assetsHolder.getAsset(limitAsset).accuracy, false)
            if (order.clientId == limitOrder.clientId) {
                skipLimitOrders.add(limitOrder)
            } else if (genericLimitOrderService.isEnoughFunds(limitOrder, volume) && limitBalance >= limitVolume) {
                matchedOrders.add(limitOrder)
                remainingVolume -= getVolume(volume, order.isStraight(), limitOrder.price)
                totalVolume += volume
                totalLimitPrice += volume * limitOrder.price
                limitBalances[limitOrder.clientId] = limitBalance - limitVolume
            } else {
                cancelledLimitOrders.add(limitOrder)
            }
        }

        if (order.takePrice() == null && remainingVolume.greaterThan(0.0)) {
            order.status = OrderStatus.NoLiquidity.name
            LOGGER.info("No liquidity, not enough funds on limit orders, for market order id: ${order.externalId}}, client: ${order.clientId}, asset: ${order.assetPairId}, volume: ${RoundingUtils.roundForPrint(order.volume)} | Unfilled: ${RoundingUtils.roundForPrint(remainingVolume)}, price: ${order.takePrice()}")
            return MatchingResult(order)
        }

        val isBuy = order.isBuySide()
        val asset = assetsHolder.getAsset(if (isBuy) assetPair.quotingAssetId else assetPair.baseAssetId)
        val limitAsset = assetsHolder.getAsset(if (isBuy) assetPair.baseAssetId else assetPair.quotingAssetId)

        if (order.calculateReservedVolume() > availableBalance) {
            order.status = OrderStatus.ReservedVolumeGreaterThanBalance.name
            LOGGER.info("Reserved volume (${order.calculateReservedVolume()}) greater than balance ($availableBalance) for order id: ${order.externalId}, client: ${order.clientId}, asset: ${order.assetPairId}, volume: ${RoundingUtils.roundForPrint(order.volume)}, price: ${order.takePrice()}")
            return MatchingResult(order)
        }

        val reservedBalance = if (order.calculateReservedVolume() > 0.0)  RoundingUtils.round(order.calculateReservedVolume(), asset.accuracy, true) else availableBalance
        if (reservedBalance < RoundingUtils.round( if(isBuy) totalLimitPrice else totalVolume, asset.accuracy, true)) {
            order.status = OrderStatus.NotEnoughFunds.name
            LOGGER.info("Not enough funds for order id: ${order.externalId}, client: ${order.clientId}, asset: ${order.assetPairId}, volume: ${RoundingUtils.roundForPrint(order.volume)}, price: ${order.takePrice()} : $reservedBalance < ${RoundingUtils.round( if(isBuy) totalLimitPrice else totalVolume, asset.accuracy, true)}")
            return MatchingResult(order)
        }

        remainingVolume = order.getAbsVolume()
        totalLimitPrice = 0.0
        var totalLimitVolume = 0.0
        totalVolume = 0.0
        val now = Date()

        val completedLimitOrders = LinkedList<NewLimitOrder>()
        var uncompletedLimitOrder: NewLimitOrder? = null
        val lkkTrades = LinkedList<LkkTrade>()
        val cashMovements = LinkedList<WalletOperation>()

        var marketBalance = availableBalance

        val marketOrderTrades = LinkedList<TradeInfo>()

        val limitOrdersReport = LimitOrdersReport()

        matchedOrders.forEachIndexed { index, limitOrder ->
            if (remainingVolume > 0.0) {
                val limitRemainingVolume = limitOrder.getAbsRemainingVolume()
                val marketRemainingVolume = getCrossVolume(remainingVolume, order.isStraight(), limitOrder.price)
                val volume = if (marketRemainingVolume >= limitRemainingVolume) limitRemainingVolume else marketRemainingVolume

                var marketRoundedVolume = RoundingUtils.round(if (isBuy) volume else -volume, assetsHolder.getAsset(assetPair.baseAssetId).accuracy, order.isOrigBuySide())
                var oppositeRoundedVolume = RoundingUtils.round(if (isBuy) -limitOrder.price * volume else limitOrder.price * volume, assetsHolder.getAsset(assetPair.quotingAssetId).accuracy, isBuy)

                LOGGER.info("Matching with limit order ${limitOrder.externalId}, client ${limitOrder.clientId}, price ${limitOrder.price}, " +
                        "marketVolume ${RoundingUtils.roundForPrint(if (isBuy) oppositeRoundedVolume else marketRoundedVolume)}, " +
                        "limitVolume ${RoundingUtils.roundForPrint(if (isBuy) marketRoundedVolume else oppositeRoundedVolume)}")

                if ((!order.isStraight()) && (index == matchedOrders.size - 1)) {
                    oppositeRoundedVolume = Math.signum(order.volume) * (RoundingUtils.round(Math.abs(order.volume), assetsHolder.getAsset(assetPair.quotingAssetId).accuracy, isBuy) - Math.abs(totalLimitVolume))
                    marketRoundedVolume = RoundingUtils.round(-oppositeRoundedVolume / limitOrder.price, assetsHolder.getAsset(assetPair.baseAssetId).accuracy, order.isOrigBuySide())
                    LOGGER.debug("Rounding last matched limit order trade: ${RoundingUtils.roundForPrint(marketRoundedVolume)}")
                }

                if (isBuy) {
                    if (marketBalance < Math.abs(oppositeRoundedVolume)) {
                        oppositeRoundedVolume = Math.signum(oppositeRoundedVolume) * marketBalance
                        LOGGER.debug("Adjusting market volume due to low balance and rounding: ${RoundingUtils.roundForPrint(oppositeRoundedVolume)}")
                    }
                } else {
                    if (marketBalance < Math.abs(marketRoundedVolume)) {
                        marketRoundedVolume = Math.signum(marketRoundedVolume) * marketBalance
                        LOGGER.debug("Adjusting market volume due to low balance and rounding: ${RoundingUtils.roundForPrint(marketRoundedVolume)}")
                    }
                }

                LOGGER.debug("Corrected volumes: " +
                        "marketVolume ${RoundingUtils.roundForPrint(if (isBuy) oppositeRoundedVolume else marketRoundedVolume)}, " +
                        "limitVolume ${RoundingUtils.roundForPrint(if (isBuy) marketRoundedVolume else oppositeRoundedVolume)}")

                cashMovements.add(WalletOperation(UUID.randomUUID().toString(), null, order.clientId, assetPair.baseAssetId, now, marketRoundedVolume, 0.0))
                cashMovements.add(WalletOperation(UUID.randomUUID().toString(), null, order.clientId, assetPair.quotingAssetId, now, oppositeRoundedVolume, 0.0))
                cashMovements.add(WalletOperation(UUID.randomUUID().toString(), null, limitOrder.clientId, assetPair.baseAssetId, now, -marketRoundedVolume, if (-marketRoundedVolume < 0) -marketRoundedVolume else 0.0))
                cashMovements.add(WalletOperation(UUID.randomUUID().toString(), null, limitOrder.clientId, assetPair.quotingAssetId, now, -oppositeRoundedVolume, if (-oppositeRoundedVolume < 0) -oppositeRoundedVolume else 0.0))

                val limitVolumeAsset = assetsHolder.getAsset(assetsPairsHolder.getAssetPair(limitOrder.assetPairId).baseAssetId)
                val marketVolumeAsset = assetsHolder.getAsset(assetsPairsHolder.getAssetPair(limitOrder.assetPairId).quotingAssetId)
                if (limitOrder.reservedLimitVolume != null && limitOrder.reservedLimitVolume!! > 0) {
                    limitOrder.reservedLimitVolume =  RoundingUtils.parseDouble(limitOrder.reservedLimitVolume!! + if (-marketRoundedVolume < 0) -marketRoundedVolume else -oppositeRoundedVolume, if (-marketRoundedVolume < 0) limitAsset.accuracy else marketVolumeAsset.accuracy).toDouble()
                }

                if (RoundingUtils.parseDouble(limitOrder.remainingVolume + marketRoundedVolume, limitVolumeAsset.accuracy).toDouble() == 0.0) {
                    lkkTrades.add(LkkTrade(limitOrder.assetPairId, limitOrder.price, limitOrder.remainingVolume, now))
                    limitOrder.remainingVolume = 0.0
                    limitOrder.status = OrderStatus.Matched.name
                    completedLimitOrders.add(limitOrder)
                    if (limitOrder.reservedLimitVolume != null && limitOrder.reservedLimitVolume!! > 0) {
                        cashMovements.add(WalletOperation(UUID.randomUUID().toString(), null, limitOrder.clientId, if (-marketRoundedVolume < 0) assetPair.baseAssetId else assetPair.quotingAssetId, now, 0.0, -limitOrder.reservedLimitVolume!!))
                        limitOrder.reservedLimitVolume =  0.0
                    }
                } else {
                    lkkTrades.add(LkkTrade(limitOrder.assetPairId, limitOrder.price, -marketRoundedVolume, now))
                    limitOrder.remainingVolume = RoundingUtils.parseDouble(limitOrder.remainingVolume + marketRoundedVolume, limitVolumeAsset.accuracy).toDouble()
                    limitOrder.status = OrderStatus.Processing.name
                    uncompletedLimitOrder = limitOrder
                }

                remainingVolume = RoundingUtils.round(remainingVolume - getVolume(Math.abs(marketRoundedVolume), order.isStraight(), limitOrder.price), assetsHolder.getAsset(assetPair.baseAssetId).accuracy, order.isOrigBuySide())
                limitOrder.lastMatchTime = now

                marketOrderTrades.add(TradeInfo(order.clientId, Math.abs(if (isBuy) oppositeRoundedVolume else marketRoundedVolume).round(asset.accuracy), asset.assetId,
                        limitOrder.clientId, Math.abs(if (isBuy) marketRoundedVolume else oppositeRoundedVolume).round(limitAsset.accuracy), limitAsset.assetId,
                        limitOrder.price, limitOrder.id, limitOrder.externalId, now))
                limitOrdersReport.orders.add(LimitOrderWithTrades(limitOrder, mutableListOf(LimitTradeInfo(limitOrder.clientId, limitAsset.assetId, Math.abs(if (isBuy) marketRoundedVolume else oppositeRoundedVolume).round(limitAsset.accuracy), limitOrder.price, now,
                        order.id, order.externalId, asset.assetId, order.clientId, Math.abs(if (isBuy) oppositeRoundedVolume else marketRoundedVolume).round(asset.accuracy)))))
                totalVolume += volume
                totalLimitPrice += volume * limitOrder.price
                totalLimitVolume += Math.abs(if (order.isStraight()) marketRoundedVolume else oppositeRoundedVolume)
                marketBalance = RoundingUtils.parseDouble(marketBalance - Math.abs(if (isBuy) oppositeRoundedVolume else marketRoundedVolume), asset.accuracy).toDouble()
            }
        }

        if (order.takePrice() != null && remainingVolume > 0.0) {
            order.status = OrderStatus.Processing.name
            order.updateRemainingVolume(if (order.isBuySide() || remainingVolume == 0.0) remainingVolume else -remainingVolume)
        } else {
            order.status = OrderStatus.Matched.name
            order.updateRemainingVolume(0.0)
        }
        order.updateMatchTime(now)
        order.updatePrice(RoundingUtils.round(if (order.isStraight()) totalLimitPrice / order.getAbsVolume() else order.getAbsVolume() / totalVolume
                , assetsPairsHolder.getAssetPair(order.assetPairId).accuracy, order.isOrigBuySide()))

        return MatchingResult(order, cancelledLimitOrders, skipLimitOrders, completedLimitOrders, uncompletedLimitOrder, lkkTrades, cashMovements, marketOrderTrades, limitOrdersReport, workingOrderBook, marketBalance)
    }

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
}
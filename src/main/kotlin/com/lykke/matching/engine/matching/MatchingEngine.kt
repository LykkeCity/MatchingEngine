package com.lykke.matching.engine.matching

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.LkkTrade
import com.lykke.matching.engine.daos.Order
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
import com.lykke.matching.engine.services.MarketOrderService
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

    fun match(order: Order, orderBook: PriorityBlockingQueue<LimitOrder>): MatchingResult {
        var remainingVolume = order.getAbsVolume()
        val matchedOrders = LinkedList<LimitOrder>()
        val skipLimitOrders = HashSet<LimitOrder>()
        val cancelledLimitOrders = HashSet<LimitOrder>()

        var totalLimitPrice = 0.0
        var totalVolume = 0.0
        val limitBalances = HashMap<String, Double>()

        val assetPair = assetsPairsHolder.getAssetPair(order.assetPairId)

        while (orderBook.size > 0 && ((order.takePrice() == null && remainingVolume.greaterThan(0.0))
                || (order.takePrice() != null && if (order.isBuySide()) order.takePrice()!! >= orderBook.peek().price else order.takePrice()!! <= orderBook.peek().price ))
                ) {
            val limitOrder = orderBook.poll()
            val limitRemainingVolume = limitOrder.getAbsRemainingVolume()
            val marketRemainingVolume = getCrossVolume(remainingVolume, order.isStraight(), limitOrder.price)
            val volume = if (marketRemainingVolume >= limitRemainingVolume) limitRemainingVolume else marketRemainingVolume
            val limitBalance = limitBalances[limitOrder.clientId] ?: balancesHolder.getAvailableBalance(limitOrder.clientId, if (limitOrder.isBuySide()) assetPair.quotingAssetId else assetPair.baseAssetId)
            val limitVolume = Math.abs(if (limitOrder.isBuySide()) volume * limitOrder.price else volume)
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

        val isProcessing = order.takePrice() != null && remainingVolume > 0

        if (order.takePrice() == null && remainingVolume.greaterThan(0.0)) {
            order.status = OrderStatus.NoLiquidity.name
            LOGGER.info("No liquidity, not enough funds on limit orders, for market order id: ${order.externalId}}, client: ${order.clientId}, asset: ${order.assetPairId}, volume: ${RoundingUtils.roundForPrint(order.volume)} | Unfilled: ${RoundingUtils.roundForPrint(remainingVolume)}")
            return MatchingResult(order)
        }

        val isBuy = order.isBuySide()
        val asset = assetsHolder.getAsset(if (isBuy) assetPair.quotingAssetId else assetPair.baseAssetId)
        val limitAsset = assetsHolder.getAsset(if (isBuy) assetPair.baseAssetId else assetPair.quotingAssetId)

        if (order.calculateReservedVolume() > getBalance(order)) {
            order.status = OrderStatus.ReservedVolumeGreaterThanBalance.name
            LOGGER.info("Not enough funds for market order id: ${order.externalId}}, client: ${order.clientId}, asset: ${order.assetPairId}, volume: ${RoundingUtils.roundForPrint(order.volume)}")
            return MatchingResult(order)
        }

        val balance = if (order.calculateReservedVolume() > 0.0) order.calculateReservedVolume() else getBalance(order)
        if (balance < RoundingUtils.round( if(isBuy) totalLimitPrice else totalVolume, asset.accuracy, true)) {
            order.status = OrderStatus.NotEnoughFunds.name
            LOGGER.info("Not enough funds for market order id: ${order.id}}, client: ${order.clientId}, asset: ${order.assetPairId}, volume: ${RoundingUtils.roundForPrint(order.volume)}")
            return MatchingResult(order)
        }

        if (asset.dustLimit!= null && asset.dustLimit > 0.0) {
            val orderBalance = balancesHolder.getBalance(order.clientId, asset.assetId)
            if (order.isStraight() && isBuy) {
                if (orderBalance - Math.abs(totalLimitPrice) < asset.dustLimit) {
                    order.dustSize = RoundingUtils.parseDouble((orderBalance - Math.abs(totalLimitPrice)), asset.accuracy).toDouble()
                    order.volume = RoundingUtils.parseDouble(Math.signum(order.volume) * (Math.abs(order.volume) + order.dustSize!!), asset.accuracy).toDouble()
                }
            } else if (order.isStraight() && !isBuy) {
                if (orderBalance - Math.abs(order.volume) < asset.dustLimit) {
                    order.dustSize = RoundingUtils.parseDouble(orderBalance - Math.abs(order.volume), asset.accuracy).toDouble()
                    order.volume = if (order.isOrigBuySide()) orderBalance else - orderBalance
                }
            } else if (!order.isStraight()) {
                val marketVolume = if(isBuy) totalLimitPrice else totalVolume
                val lastLimitOrder = matchedOrders.last
                if (orderBalance - Math.abs(marketVolume) < asset.dustLimit) {
                    order.dustSize = RoundingUtils.parseDouble((orderBalance - Math.abs(marketVolume)) * lastLimitOrder.price, assetPair.accuracy).toDouble()
                    order.volume = RoundingUtils.parseDouble(Math.signum(order.volume) * (Math.abs(order.volume) + order.dustSize!!), assetPair.accuracy).toDouble()
                }
            }
        }

        remainingVolume = order.getAbsVolume()
        totalLimitPrice = 0.0
        var totalLimitVolume = 0.0
        totalVolume = 0.0
        val now = Date()

        val completedLimitOrders = LinkedList<LimitOrder>()
        var uncompletedLimitOrder: LimitOrder? = null
        val lkkTrades = LinkedList<LkkTrade>()
        val cashMovements = LinkedList<WalletOperation>()

        var marketBalance = balancesHolder.getBalance(order.clientId, if (isBuy) assetPair.quotingAssetId else assetPair.baseAssetId)

        val marketOrderTrades = LinkedList<TradeInfo>()

        val limitOrdersReport = LimitOrdersReport()

        matchedOrders.forEachIndexed { index, limitOrder ->
            val limitRemainingVolume = limitOrder.getAbsRemainingVolume()
            val marketRemainingVolume = getCrossVolume(remainingVolume, order.isStraight(), limitOrder.price)
            val volume = if (marketRemainingVolume >= limitRemainingVolume) limitRemainingVolume else marketRemainingVolume

            var marketRoundedVolume = RoundingUtils.round(if (isBuy) volume else -volume, assetsHolder.getAsset(assetPair.baseAssetId).accuracy, order.isOrigBuySide())
            var oppositeRoundedVolume = RoundingUtils.round(if (isBuy) -limitOrder.price * volume else limitOrder.price * volume, assetsHolder.getAsset(assetPair.quotingAssetId).accuracy, isBuy)

            LOGGER.info("Matching with limit order ${limitOrder.id}, price ${limitOrder.price}, " +
                    "marketVolume ${RoundingUtils.roundForPrint(if (isBuy) oppositeRoundedVolume else marketRoundedVolume)}, " +
                    "limitVolume ${RoundingUtils.roundForPrint(if (isBuy) marketRoundedVolume else oppositeRoundedVolume)}")

            if ((!order.isStraight()) && (index == matchedOrders.size - 1)) {
                oppositeRoundedVolume = Math.signum(order.volume) * (RoundingUtils.round(Math.abs(order.volume), assetsHolder.getAsset(assetPair.quotingAssetId).accuracy, isBuy) - Math.abs(totalLimitVolume))
                marketRoundedVolume = RoundingUtils.round(- oppositeRoundedVolume / limitOrder.price, assetsHolder.getAsset(assetPair.baseAssetId).accuracy, order.isOrigBuySide())
                MarketOrderService.LOGGER.debug("Rounding last matched limit order trade: ${RoundingUtils.roundForPrint(marketRoundedVolume)}")
            }

            //in case of non-straight orders, avoid negative holders due to rounding of asset pair
            if (asset.dustLimit != null && asset.dustLimit > 0 && marketBalance > 0.0 && marketBalance - Math.abs(if (isBuy) oppositeRoundedVolume else marketRoundedVolume) < asset.dustLimit) {
                if (isBuy) {
                    oppositeRoundedVolume = Math.signum(oppositeRoundedVolume) * marketBalance
                    LOGGER.debug("Adjusting volume due to dust: ${RoundingUtils.roundForPrint(oppositeRoundedVolume)}")
                } else {
                    marketRoundedVolume = Math.signum(marketRoundedVolume) * marketBalance
                    LOGGER.debug("Adjusting volume due to dust: ${RoundingUtils.roundForPrint(marketRoundedVolume)}")
                }
            }

            if (isBuy) {
                if (marketBalance < Math.abs(oppositeRoundedVolume)) {
                    oppositeRoundedVolume = Math.signum(oppositeRoundedVolume) * marketBalance
                    LOGGER.debug("Adjusting volume due to low balance and rounding: ${RoundingUtils.roundForPrint(oppositeRoundedVolume)}")
                }
            } else {
                if (marketBalance < Math.abs(marketRoundedVolume)) {
                    marketRoundedVolume = Math.signum(marketRoundedVolume) * marketBalance
                    LOGGER.debug("Adjusting volume due to low balance and rounding: ${RoundingUtils.roundForPrint(marketRoundedVolume)}")
                }
            }

            LOGGER.debug("Corrected volumes: " +
                    "marketVolume ${RoundingUtils.roundForPrint(if (isBuy) oppositeRoundedVolume else marketRoundedVolume)}, " +
                    "limitVolume ${RoundingUtils.roundForPrint(if (isBuy) marketRoundedVolume else oppositeRoundedVolume)}")

            //check dust
            if (asset.dustLimit != null && Math.abs(if (isBuy) oppositeRoundedVolume else marketRoundedVolume) < asset.dustLimit) {
                order.status = OrderStatus.Dust.name
                LOGGER.info("Market volume ${RoundingUtils.roundForPrint(if (isBuy) oppositeRoundedVolume else marketRoundedVolume)} is less than dust ${RoundingUtils.roundForPrint(asset.dustLimit)}. id: ${order.externalId}}, client: ${order.clientId}, asset: ${order.assetPairId}, volume: ${RoundingUtils.roundForPrint(volume)}")
                return MatchingResult(order)
            }
            if (limitAsset.dustLimit != null && Math.abs(if (isBuy) marketRoundedVolume else oppositeRoundedVolume) < limitAsset.dustLimit) {
                order.status = OrderStatus.Dust.name
                LOGGER.info("Limit volume ${RoundingUtils.roundForPrint(if (isBuy) marketRoundedVolume else oppositeRoundedVolume)} is less than dust ${RoundingUtils.roundForPrint(limitAsset.dustLimit)}. id: ${order.externalId}}, client: ${order.clientId}, asset: ${order.assetPairId}, volume: ${RoundingUtils.roundForPrint(order.volume)}")
                return MatchingResult(order)
            }

            cashMovements.add(WalletOperation(UUID.randomUUID().toString(), null, order.clientId, assetPair.baseAssetId, now, marketRoundedVolume, 0.0))
            cashMovements.add(WalletOperation(UUID.randomUUID().toString(), null, order.clientId, assetPair.quotingAssetId, now, oppositeRoundedVolume, 0.0))
            cashMovements.add(WalletOperation(UUID.randomUUID().toString(), null, limitOrder.clientId, assetPair.baseAssetId, now, -marketRoundedVolume, -marketRoundedVolume))
            cashMovements.add(WalletOperation(UUID.randomUUID().toString(), null, limitOrder.clientId, assetPair.quotingAssetId, now, -oppositeRoundedVolume, -oppositeRoundedVolume))

            if (marketRemainingVolume >= limitRemainingVolume) {
                lkkTrades.add(LkkTrade(limitOrder.assetPairId, limitOrder.price, limitOrder.remainingVolume, now))
                limitOrder.remainingVolume = 0.0
                limitOrder.status = OrderStatus.Matched.name
                completedLimitOrders.add(limitOrder)
            } else {
                lkkTrades.add(LkkTrade(limitOrder.assetPairId, limitOrder.price, -marketRoundedVolume, now))
                val limitVolumeAsset = assetsHolder.getAsset(assetsPairsHolder.getAssetPair(limitOrder.assetPairId).baseAssetId)
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

        if (isProcessing) {
            order.status = OrderStatus.Processing.name
            order.updateRemainingVolume(remainingVolume)
        } else {
            order.status = OrderStatus.Matched.name
        }
        order.updateMatchTime(now)
        order.updatePrice(RoundingUtils.round(if (order.isStraight()) totalLimitPrice / order.getAbsVolume() else order.getAbsVolume() / totalVolume
                , assetsPairsHolder.getAssetPair(order.assetPairId).accuracy, order.isOrigBuySide()))

        return MatchingResult(order, cancelledLimitOrders, skipLimitOrders, completedLimitOrders, uncompletedLimitOrder, lkkTrades, cashMovements, marketOrderTrades, limitOrdersReport, orderBook)
    }

    private fun getCrossVolume(volume: Double, straight: Boolean, price: Double): Double {
        return if (straight) volume else volume / price
    }

    private fun getVolume(volume: Double, straight: Boolean, price: Double): Double {
        return if (straight) volume else volume * price
    }

    private fun getBalance(order: Order): Double {
        val assetPair = assetsPairsHolder.getAssetPair(order.assetPairId)
        val asset = if (order.isBuySide()) assetPair.quotingAssetId else assetPair.baseAssetId
        return balancesHolder.getAvailableBalance(order.clientId, asset)
    }
}
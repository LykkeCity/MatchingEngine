package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.MarketOrder
import com.lykke.matching.engine.daos.MatchedLimitOrder
import com.lykke.matching.engine.daos.MatchedMarketOrder
import com.lykke.matching.engine.daos.Trade
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.database.MarketOrderDatabaseAccessor
import com.lykke.matching.engine.greaterThan
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.order.OrderSide
import com.lykke.matching.engine.order.OrderSide.Buy
import com.lykke.matching.engine.order.OrderStatus.Matched
import com.lykke.matching.engine.order.OrderStatus.NoLiquidity
import com.lykke.matching.engine.order.OrderStatus.NotEnoughFunds
import com.lykke.matching.engine.order.OrderStatus.Processing
import com.lykke.matching.engine.order.OrderStatus.UnknownAsset
import org.apache.log4j.Logger
import java.util.Date
import java.util.HashSet
import java.util.LinkedList
import java.util.PriorityQueue
import java.util.UUID

class MarketOrderService(private val marketOrderDatabaseAccessor: MarketOrderDatabaseAccessor,
                         private val limitOrderService: LimitOrderService,
                         private val cashOperationService: CashOperationService): AbsractService<ProtocolMessages.MarketOrder> {

    companion object {
        val LOGGER = Logger.getLogger(MarketOrderService::class.java.name)
    }

    init {
    }

    override fun processMessage(array: ByteArray) {
        val message = parse(array)
        val orderSide = OrderSide.valueOf(message.orderAction)
        LOGGER.debug("Got market order id: ${message.uid}, client: ${message.clientId}, asset: ${message.assetId}, volume: ${message.volume}, side: ${orderSide?.name}")
        if (orderSide == null) {
            LOGGER.error("Unknown order action: ${message.orderAction}")
            return
        }

        val order = MarketOrder(
                rowKey = message.uid.toString(),
                assetPair = message.assetId,
                clientId = message.clientId,
                matchedAt = null,
                blockChain = message.blockChain,
                orderType = orderSide.name,
                createdAt = Date(message.timestamp),
                registered = Date(),
                status = Processing.name,
                volume = message.volume,
                matchedOrders = null
        )

        if (cashOperationService.getAssetPair(message.assetId) == null) {
            order.status = UnknownAsset.name
            marketOrderDatabaseAccessor.addMarketOrder(order)
            LOGGER.debug("Unknown asset: ${message.assetId}")
            return
        }

        val orderBook = limitOrderService.getOrderBook("${order.assetPair}_${orderSide.oppositeSide().name}")
        if (orderBook == null) {
            order.status = NoLiquidity.name
            marketOrderDatabaseAccessor.addMarketOrder(order)
            LOGGER.debug("No liquidity for market order id: ${order.getId()}}, client: ${order.getClientId()}, asset: ${order.assetPair}, volume: ${order.volume}, side: ${order.orderType}")
            return
        }

        match(order, orderBook)
    }

    private fun parse(array: ByteArray): ProtocolMessages.MarketOrder {
        return ProtocolMessages.MarketOrder.parseFrom(array)
    }

    private fun match(marketOrder: MarketOrder, orderBook: PriorityQueue<LimitOrder>) {
        var remainingVolume = marketOrder.volume
        val matchedOrders = HashSet<LimitOrder>()
        val cancelledLimitOrders = HashSet<LimitOrder>()

        var totalPrice = 0.0
        while (remainingVolume.greaterThan(0.0) && orderBook.size > 0) {
            val limitOrder = orderBook.poll()
            val volume = if (remainingVolume >= limitOrder.remainingVolume) limitOrder.remainingVolume else remainingVolume
            if (limitOrderService.isEnoughFunds(limitOrder, volume)) {
                matchedOrders.add(limitOrder)
                remainingVolume -= volume
                totalPrice += volume * limitOrder.price
            } else {
                cancelledLimitOrders.add(limitOrder)
            }
        }

        if (remainingVolume.greaterThan(0.0)) {
            marketOrder.status = NoLiquidity.name
            marketOrderDatabaseAccessor.addMarketOrder(marketOrder)
            matchedOrders.forEach { limitOrderService.addToOrderBook(it) }
            cancelledLimitOrders.forEach { limitOrderService.addToOrderBook(it) }
            LOGGER.debug("No liquidity for market order id: ${marketOrder.getId()}}, client: ${marketOrder.getClientId()}, asset: ${marketOrder.assetPair}, volume: ${marketOrder.volume}, side: ${marketOrder.orderType}")
            return
        }

        if (!isEnoughFunds(marketOrder, totalPrice)) {
            marketOrder.status = NotEnoughFunds.name
            marketOrderDatabaseAccessor.addMarketOrder(marketOrder)
            LOGGER.debug("Not enough funds for market order id: ${marketOrder.getId()}}, client: ${marketOrder.getClientId()}, asset: ${marketOrder.assetPair}, volume: ${marketOrder.volume}, side: ${marketOrder.orderType}")
            return
        }

        remainingVolume = marketOrder.volume
        val now = Date()
        val assetPair = cashOperationService.getAssetPair(marketOrder.assetPair) ?: return


        val completedLimitOrders = LinkedList<LimitOrder>()
        var uncompletedLimitOrder: LimitOrder? = null
        val marketTrades = LinkedList<Trade>()
        val limitTrades = LinkedList<Trade>()
        val cashMovements = LinkedList<WalletOperation>()

        matchedOrders.forEach { limitOrder ->
            val volume = if (remainingVolume >= limitOrder.remainingVolume) limitOrder.remainingVolume else remainingVolume
            limitOrder.addMatchedOrder(MatchedMarketOrder(Id = marketOrder.getId(), Volume = limitOrder.remainingVolume))
            marketOrder.addMatchedOrder(MatchedLimitOrder(Id = limitOrder.getId(), Volume = limitOrder.remainingVolume, Price = limitOrder.price))
            val isMarketBuy = OrderSide.valueOf(marketOrder.orderType) == Buy
            val oppositeSideVolume = limitOrder.price * volume

            marketTrades.add(Trade(partitionKey = marketOrder.getClientId(), rowKey = UUID.randomUUID().toString(),
                    assetId = assetPair.baseAssetId, dateTime = now, limitOrderId = limitOrder.getId(),
                    marketOrderId = marketOrder.getId(), volume = if (isMarketBuy) volume else -volume))
            cashMovements.add(WalletOperation(clientId = marketOrder.getClientId(), uid = UUID.randomUUID().toString(),
                    asset = assetPair.baseAssetId, dateTime = now, amount = if (isMarketBuy) volume else -volume))

            marketTrades.add(Trade(partitionKey = marketOrder.getClientId(), rowKey = UUID.randomUUID().toString(),
                    assetId = assetPair.quotingAssetId, dateTime = now, limitOrderId = limitOrder.getId(),
                    marketOrderId = marketOrder.getId(), volume = if (isMarketBuy) -oppositeSideVolume else oppositeSideVolume))
            cashMovements.add(WalletOperation(clientId = marketOrder.getClientId(), uid = UUID.randomUUID().toString(),
                    asset = assetPair.quotingAssetId, dateTime = now, amount = if (isMarketBuy) -oppositeSideVolume else oppositeSideVolume))

            limitTrades.add(Trade(partitionKey = limitOrder.clientId, rowKey = UUID.randomUUID().toString(),
                    assetId = assetPair.baseAssetId, dateTime = now, limitOrderId = limitOrder.getId(),
                    marketOrderId = marketOrder.getId(), volume = if (isMarketBuy) -volume else volume))
            cashMovements.add(WalletOperation(clientId = limitOrder.clientId, uid = UUID.randomUUID().toString(),
                    asset = assetPair.baseAssetId, dateTime = now, amount = if (isMarketBuy) -volume else volume))

            limitTrades.add(Trade(partitionKey = limitOrder.clientId, rowKey = UUID.randomUUID().toString(),
                    assetId = assetPair.quotingAssetId, dateTime = now, limitOrderId = limitOrder.getId(),
                    marketOrderId = marketOrder.getId(), volume = if (isMarketBuy) oppositeSideVolume else -oppositeSideVolume))
            cashMovements.add(WalletOperation(clientId = limitOrder.clientId, uid = UUID.randomUUID().toString(),
                    asset = assetPair.quotingAssetId, dateTime = now, amount = if (isMarketBuy) oppositeSideVolume else -oppositeSideVolume))

            if (remainingVolume >= limitOrder.remainingVolume) {
                limitOrder.remainingVolume = 0.0
                limitOrder.status = Matched.name
                completedLimitOrders.add(limitOrder)
            } else {
                limitOrder.remainingVolume -= remainingVolume
                limitOrder.status = Processing.name
                uncompletedLimitOrder = limitOrder
            }
            remainingVolume -= volume
            limitOrder.lastMatchTime = now
        }

        marketOrder.status = Matched.name
        marketOrder.matchedAt = now
        marketOrder.price = totalPrice / marketOrder.volume
        marketOrderDatabaseAccessor.addMarketOrder(marketOrder)
        marketOrderDatabaseAccessor.addTrades(marketTrades)
        marketOrderDatabaseAccessor.addTrades(limitTrades)

        cashOperationService.processWalletOperations(cashMovements)

        limitOrderService.moveOrdersToDone(completedLimitOrders)
        cancelledLimitOrders.forEach { limitOrder ->
            limitOrder.status = NotEnoughFunds.name
            limitOrderService.updateLimitOrder(limitOrder)
        }

        if (uncompletedLimitOrder != null) {
            limitOrderService.updateLimitOrder(uncompletedLimitOrder as LimitOrder)
            limitOrderService.addToOrderBook(uncompletedLimitOrder as LimitOrder)
        }

        LOGGER.debug("Market order id: ${marketOrder.getId()}}, client: ${marketOrder.getClientId()}, asset: ${marketOrder.assetPair}, volume: ${marketOrder.volume}, side: ${marketOrder.orderType} matched, price: ${marketOrder.price}")
    }

    fun isEnoughFunds(order: MarketOrder, totalPrice: Double): Boolean {
        val assetPair = cashOperationService.getAssetPair(order.assetPair) ?: return false

        when (OrderSide.valueOf(order.orderType)) {
            OrderSide.Sell -> {
                return cashOperationService.getBalance(order.getClientId(), assetPair.baseAssetId) >= order.volume
            }
            Buy -> {
                return cashOperationService.getBalance(order.getClientId(), assetPair.quotingAssetId) >= totalPrice
            }
        }
    }
}
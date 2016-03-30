package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.ClientOrderPair
import com.lykke.matching.engine.daos.ClientTradePair
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.MarketOrder
import com.lykke.matching.engine.daos.MatchingData
import com.lykke.matching.engine.daos.OrderTradesLink
import com.lykke.matching.engine.daos.Orders
import com.lykke.matching.engine.daos.Trade
import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.database.MarketOrderDatabaseAccessor
import com.lykke.matching.engine.greaterThan
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.order.OrderStatus.Matched
import com.lykke.matching.engine.order.OrderStatus.NoLiquidity
import com.lykke.matching.engine.order.OrderStatus.NotEnoughFunds
import com.lykke.matching.engine.order.OrderStatus.Processing
import com.lykke.matching.engine.order.OrderStatus.UnknownAsset
import com.lykke.matching.engine.queue.transaction.Swap
import com.lykke.matching.engine.queue.transaction.Transaction
import org.apache.log4j.Logger
import java.util.ArrayList
import java.util.Date
import java.util.HashSet
import java.util.LinkedList
import java.util.UUID
import java.util.concurrent.BlockingQueue
import java.util.concurrent.PriorityBlockingQueue

class MarketOrderService(private val marketOrderDatabaseAccessor: MarketOrderDatabaseAccessor,
                         private val limitOrderService: LimitOrderService,
                         private val cashOperationService: CashOperationService,
                         private val backendQueue: BlockingQueue<Transaction>,
                         private val tradesInfoQueue: BlockingQueue<TradeInfo>): AbsractService<ProtocolMessages.MarketOrder> {

    companion object {
        val LOGGER = Logger.getLogger(MarketOrderService::class.java.name)

        private val ORDER_ID = "OrderId"
    }

    init {
    }

    override fun processMessage(messageWrapper: MessageWrapper) {
        val message = parse(messageWrapper.byteArray)
        LOGGER.debug("Got market order id: ${message.uid}, client: ${message.clientId}, asset: ${message.assetPairId}, volume: ${message.volume}")

        val order = MarketOrder(
                uid = UUID.randomUUID().toString(),
                assetPairId = message.assetPairId,
                clientId = message.clientId,
                createdAt = Date(message.timestamp),
                registered = Date(),
                status = Processing.name,
                volume = message.volume
        )

        if (cashOperationService.getAssetPair(message.assetPairId) == null) {
            order.status = UnknownAsset.name
            marketOrderDatabaseAccessor.addMarketOrder(order)
            LOGGER.debug("Unknown asset: ${message.assetPairId}")
            messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder().setUid(message.uid).build())
            return
        }

        val orderBook = limitOrderService.getOrderBook(order.assetPairId)?.getOrderBook(!order.isBuySide())
        if (orderBook == null) {
            order.status = NoLiquidity.name
            marketOrderDatabaseAccessor.addMarketOrder(order)
            LOGGER.debug("No liquidity for market order id: ${order.getId()}}, client: ${order.clientId}, asset: ${order.assetPairId}, volume: ${order.volume}")
            messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder().setUid(message.uid).setRecordId(order.getId()).build())
            return
        }

        match(order, orderBook)
        messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder().setUid(message.uid).build())
    }

    private fun parse(array: ByteArray): ProtocolMessages.MarketOrder {
        return ProtocolMessages.MarketOrder.parseFrom(array)
    }

    private fun match(marketOrder: MarketOrder, orderBook: PriorityBlockingQueue<LimitOrder>) {
        var remainingVolume = marketOrder.getAbsVolume()
        val matchedOrders = LinkedList<LimitOrder>()
        val cancelledLimitOrders = HashSet<LimitOrder>()

        var totalPrice = 0.0
        while (remainingVolume.greaterThan(0.0) && orderBook.size > 0) {
            val limitOrder = orderBook.poll()
            val limitRemainingVolume = limitOrder.getAbsRemainingVolume()
            val volume = if (remainingVolume >= limitRemainingVolume) limitRemainingVolume else remainingVolume
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
            LOGGER.debug("No liquidity for market order id: ${marketOrder.getId()}}, client: ${marketOrder.clientId}, asset: ${marketOrder.assetPairId}, volume: ${marketOrder.volume}")
            return
        }

        if (!isEnoughFunds(marketOrder, totalPrice)) {
            marketOrder.status = NotEnoughFunds.name
            marketOrderDatabaseAccessor.addMarketOrder(marketOrder)
            LOGGER.debug("Not enough funds for market order id: ${marketOrder.getId()}}, client: ${marketOrder.clientId}, asset: ${marketOrder.assetPairId}, volume: ${marketOrder.volume}")
            return
        }

        remainingVolume = marketOrder.getAbsVolume()
        val now = Date()
        val assetPair = cashOperationService.getAssetPair(marketOrder.assetPairId) ?: return


        val completedLimitOrders = LinkedList<LimitOrder>()
        var uncompletedLimitOrder: LimitOrder? = null
        val marketTrades = LinkedList<Trade>()
        val limitTrades = LinkedList<Trade>()
        val cashMovements = LinkedList<WalletOperation>()
        val bitcoinTransactions = LinkedList<Transaction>()
        val transactionIds = LinkedList<String>()
        val matchingData = LinkedList<MatchingData>()
        val orderTradesLinks = LinkedList<OrderTradesLink>()
        val tradesInfo = LinkedList<TradeInfo>()

        matchedOrders.forEach { limitOrder ->
            val limitRemainingVolume = limitOrder.getAbsRemainingVolume()
            val volume = if (remainingVolume >= limitRemainingVolume) limitRemainingVolume else remainingVolume
            matchingData.add(MatchingData(masterOrderId = marketOrder.getId(), matchedOrderId = limitOrder.getId(), volume = volume))
            matchingData.add(MatchingData(masterOrderId = limitOrder.getId(), matchedOrderId = marketOrder.getId(), volume = volume))
            val isMarketBuy = marketOrder.isBuySide()
            val oppositeSideVolume = limitOrder.price * volume

            val clientTradePairs = ArrayList<ClientTradePair>()

            var uid = UUID.randomUUID().toString()
            marketTrades.add(Trade(partitionKey = marketOrder.clientId, rowKey = uid,
                    assetId = assetPair.baseAssetId, dateTime = now, limitOrderId = limitOrder.getId(),
                    marketOrderId = marketOrder.getId(), volume = if (isMarketBuy) volume else -volume))
            orderTradesLinks.add(OrderTradesLink(orderId = marketOrder.getId(), tradeId = uid))
            orderTradesLinks.add(OrderTradesLink(orderId = limitOrder.getId(), tradeId = uid))
            cashMovements.add(WalletOperation(clientId = marketOrder.clientId, uid = UUID.randomUUID().toString(),
                    asset = assetPair.baseAssetId, dateTime = now, amount = if (isMarketBuy) volume else -volume))
            clientTradePairs.add(ClientTradePair(marketOrder.clientId, uid))

            uid = UUID.randomUUID().toString()
            marketTrades.add(Trade(partitionKey = marketOrder.clientId, rowKey = uid,
                    assetId = assetPair.quotingAssetId, dateTime = now, limitOrderId = limitOrder.getId(),
                    marketOrderId = marketOrder.getId(), volume = if (isMarketBuy) -oppositeSideVolume else oppositeSideVolume))
            orderTradesLinks.add(OrderTradesLink(orderId = marketOrder.getId(), tradeId = uid))
            orderTradesLinks.add(OrderTradesLink(orderId = limitOrder.getId(), tradeId = uid))
            cashMovements.add(WalletOperation(clientId = marketOrder.clientId, uid = UUID.randomUUID().toString(),
                    asset = assetPair.quotingAssetId, dateTime = now, amount = if (isMarketBuy) -oppositeSideVolume else oppositeSideVolume))
            clientTradePairs.add(ClientTradePair(marketOrder.clientId, uid))

            uid = UUID.randomUUID().toString()
            limitTrades.add(Trade(partitionKey = limitOrder.clientId, rowKey = uid,
                    assetId = assetPair.baseAssetId, dateTime = now, limitOrderId = limitOrder.getId(),
                    marketOrderId = marketOrder.getId(), volume = if (isMarketBuy) -volume else volume))
            orderTradesLinks.add(OrderTradesLink(orderId = marketOrder.getId(), tradeId = uid))
            orderTradesLinks.add(OrderTradesLink(orderId = limitOrder.getId(), tradeId = uid))
            cashMovements.add(WalletOperation(clientId = limitOrder.clientId, uid = UUID.randomUUID().toString(),
                    asset = assetPair.baseAssetId, dateTime = now, amount = if (isMarketBuy) -volume else volume))
            clientTradePairs.add(ClientTradePair(limitOrder.clientId, uid))

            uid = UUID.randomUUID().toString()
            limitTrades.add(Trade(partitionKey = limitOrder.clientId, rowKey = uid,
                    assetId = assetPair.quotingAssetId, dateTime = now, limitOrderId = limitOrder.getId(),
                    marketOrderId = marketOrder.getId(), volume = if (isMarketBuy) oppositeSideVolume else -oppositeSideVolume))
            orderTradesLinks.add(OrderTradesLink(orderId = marketOrder.getId(), tradeId = uid))
            orderTradesLinks.add(OrderTradesLink(orderId = limitOrder.getId(), tradeId = uid))
            cashMovements.add(WalletOperation(clientId = limitOrder.clientId, uid = UUID.randomUUID().toString(),
                    asset = assetPair.quotingAssetId, dateTime = now, amount = if (isMarketBuy) oppositeSideVolume else -oppositeSideVolume))
            clientTradePairs.add(ClientTradePair(limitOrder.clientId, uid))

            if (remainingVolume >= limitRemainingVolume) {
                limitOrder.remainingVolume = 0.0
                limitOrder.status = Matched.name
                completedLimitOrders.add(limitOrder)
            } else {
                limitOrder.remainingVolume -= if (limitOrder.isBuySide()) remainingVolume else -remainingVolume
                limitOrder.status = Processing.name
                uncompletedLimitOrder = limitOrder
            }
            remainingVolume -= volume
            val transactionId = UUID.randomUUID().toString()
            limitOrder.lastMatchTime = now
            limitOrder.addTransactionIds(listOf(transactionId))
            transactionIds.add(transactionId)
            tradesInfo.add(TradeInfo(assetPair = assetPair.getAssetPairId(), isBuy = marketOrder.isBuySide(), price = limitOrder.price, date = now))

            bitcoinTransactions.add(Swap(TransactionId = transactionId,
                                         clientId1 = marketOrder.clientId, Amount1 = if (isMarketBuy) oppositeSideVolume else volume, origAsset1 = if (isMarketBuy) assetPair.quotingAssetId else assetPair.baseAssetId,
                                         clientId2 = limitOrder.clientId, Amount2 = if (isMarketBuy) volume else oppositeSideVolume, origAsset2 = if (isMarketBuy) assetPair.baseAssetId else assetPair.quotingAssetId,
                                         orders = Orders(ClientOrderPair(marketOrder.clientId, marketOrder.getId()), ClientOrderPair(limitOrder.clientId, limitOrder.getId()),
                                                 clientTradePairs.toTypedArray())))
        }

        marketOrder.status = Matched.name
        marketOrder.matchedAt = now
        marketOrder.price = totalPrice / marketOrder.getAbsVolume()
        marketOrder.partitionKey = ORDER_ID
        marketOrder.addTransactionIds(transactionIds)
        marketOrderDatabaseAccessor.addMarketOrder(marketOrder)
        marketOrderDatabaseAccessor.addMarketOrderWithGeneratedRowId(marketOrder)

        marketOrderDatabaseAccessor.addTrades(marketTrades)
        marketOrderDatabaseAccessor.addTrades(limitTrades)
        marketOrderDatabaseAccessor.addMatchingData(matchingData)
        marketOrderDatabaseAccessor.addOrderTradesLinks(orderTradesLinks)

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

        bitcoinTransactions.forEach { backendQueue.put(it) }
        tradesInfo.forEach { tradesInfoQueue.put(it) }

        LOGGER.debug("Market order id: ${marketOrder.getId()}}, client: ${marketOrder.clientId}, asset: ${marketOrder.assetPairId}, volume: ${marketOrder.volume} matched, price: ${marketOrder.price}")
    }

    fun isEnoughFunds(order: MarketOrder, totalPrice: Double): Boolean {
        val assetPair = cashOperationService.getAssetPair(order.assetPairId) ?: return false

        if (order.isBuySide()) {
            return cashOperationService.getBalance(order.clientId, assetPair.quotingAssetId) >= totalPrice
        } else {
            return cashOperationService.getBalance(order.clientId, assetPair.baseAssetId) >= order.getAbsVolume()
        }
    }
}
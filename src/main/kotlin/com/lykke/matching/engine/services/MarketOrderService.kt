package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.*
import com.lykke.matching.engine.daos.Order.Companion.buildPartitionKey
import com.lykke.matching.engine.database.MarketOrderDatabaseAccessor
import com.lykke.matching.engine.greaterThan
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.order.OrderStatus.*
import com.lykke.matching.engine.queue.transaction.Swap
import com.lykke.matching.engine.queue.transaction.Transaction
import org.apache.log4j.Logger
import java.util.*
import java.util.concurrent.BlockingQueue

class MarketOrderService(private val marketOrderDatabaseAccessor: MarketOrderDatabaseAccessor,
                         private val limitOrderService: LimitOrderService,
                         private val cashOperationService: CashOperationService,
                         private val backendQueue: BlockingQueue<Transaction>): AbsractService<ProtocolMessages.MarketOrder> {

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
                uid = Date().time.toString(),
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

        val orderBook = limitOrderService.getOrderBook(buildPartitionKey(order.assetPairId, order.getSide().oppositeSide()))
        if (orderBook == null) {
            order.status = NoLiquidity.name
            marketOrderDatabaseAccessor.addMarketOrder(order)
            LOGGER.debug("No liquidity for market order id: ${order.getId()}}, client: ${order.clientId}, asset: ${order.assetPairId}, volume: ${order.volume}")
            messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder().setUid(message.uid).build())
            return
        }

        match(order, orderBook)
        messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder().setUid(message.uid).build())
    }

    private fun parse(array: ByteArray): ProtocolMessages.MarketOrder {
        return ProtocolMessages.MarketOrder.parseFrom(array)
    }

    private fun match(marketOrder: MarketOrder, orderBook: PriorityQueue<LimitOrder>) {
        var remainingVolume = marketOrder.getAbsVolume()
        val matchedOrders = LinkedList<LimitOrder>()
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
        val transactionIds = ArrayList<String>()

        matchedOrders.forEach { limitOrder ->
            val volume = if (remainingVolume >= limitOrder.remainingVolume) limitOrder.remainingVolume else remainingVolume
            limitOrder.addMatchedOrder(MatchedMarketOrder(Id = marketOrder.getId(), Volume = limitOrder.remainingVolume))
            marketOrder.addMatchedOrder(MatchedLimitOrder(Id = limitOrder.getId(), Volume = limitOrder.remainingVolume, Price = limitOrder.price))
            val isMarketBuy = marketOrder.isBuySide()
            val oppositeSideVolume = limitOrder.price * volume

            marketTrades.add(Trade(partitionKey = marketOrder.clientId, rowKey = UUID.randomUUID().toString(),
                    assetId = assetPair.baseAssetId, dateTime = now, limitOrderId = limitOrder.getId(),
                    marketOrderId = marketOrder.getId(), volume = if (isMarketBuy) volume else -volume))
            cashMovements.add(WalletOperation(clientId = marketOrder.clientId, uid = UUID.randomUUID().toString(),
                    asset = assetPair.baseAssetId, dateTime = now, amount = if (isMarketBuy) volume else -volume))

            marketTrades.add(Trade(partitionKey = marketOrder.clientId, rowKey = UUID.randomUUID().toString(),
                    assetId = assetPair.quotingAssetId, dateTime = now, limitOrderId = limitOrder.getId(),
                    marketOrderId = marketOrder.getId(), volume = if (isMarketBuy) -oppositeSideVolume else oppositeSideVolume))
            cashMovements.add(WalletOperation(clientId = marketOrder.clientId, uid = UUID.randomUUID().toString(),
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
            val transactionId = UUID.randomUUID().toString()
            limitOrder.lastMatchTime = now
            limitOrder.addTransactionIds(listOf(transactionId))
            transactionIds.add(transactionId)

            bitcoinTransactions.add(Swap(TransactionId = transactionId,
                                         clientId1 = marketOrder.clientId, Amount1 = volume, origAsset1 = if (isMarketBuy) assetPair.quotingAssetId else assetPair.baseAssetId,
                                         clientId2 = limitOrder.clientId, Amount2 = oppositeSideVolume, origAsset2 = if (isMarketBuy) assetPair.baseAssetId else assetPair.quotingAssetId,
                                         orders = Orders(ClientOrderPair(marketOrder.clientId, marketOrder.getId()), ClientOrderPair(limitOrder.clientId, limitOrder.getId()))))
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
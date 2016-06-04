package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.MarketOrder
import com.lykke.matching.engine.daos.MatchingData
import com.lykke.matching.engine.daos.OrderTradesLink
import com.lykke.matching.engine.daos.Trade
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.daos.bitcoin.ClientOrderPair
import com.lykke.matching.engine.daos.bitcoin.ClientTradePair
import com.lykke.matching.engine.daos.bitcoin.Orders
import com.lykke.matching.engine.database.MarketOrderDatabaseAccessor
import com.lykke.matching.engine.greaterThan
import com.lykke.matching.engine.logging.AMOUNT
import com.lykke.matching.engine.logging.ASSET_PAIR
import com.lykke.matching.engine.logging.CLIENT_ID
import com.lykke.matching.engine.logging.ID
import com.lykke.matching.engine.logging.KeyValue
import com.lykke.matching.engine.logging.Line
import com.lykke.matching.engine.logging.ME_MARKET_ORDER
import com.lykke.matching.engine.logging.MetricsLogger
import com.lykke.matching.engine.logging.STATUS
import com.lykke.matching.engine.logging.STRAIGHT
import com.lykke.matching.engine.logging.TIMESTAMP
import com.lykke.matching.engine.logging.UID
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
import java.time.LocalDateTime
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
                         private val backendQueue: BlockingQueue<Transaction>): AbsractService<ProtocolMessages.MarketOrder> {

    companion object {
        val LOGGER = Logger.getLogger(MarketOrderService::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()

        private val ORDER_ID = "OrderId"
    }

    init {
    }

    override fun processMessage(messageWrapper: MessageWrapper) {
        val message = parse(messageWrapper.byteArray)
        LOGGER.debug("Got market order id: ${message.uid}, client: ${message.clientId}, asset: ${message.assetPairId}, volume: ${message.volume}, straight: ${message.straight}")

        val order = MarketOrder(UUID.randomUUID().toString(), message.assetPairId, message.clientId, message.volume, null,
                Processing.name, Date(message.timestamp), Date(), null, null, message.straight)

        if (cashOperationService.getAssetPair(message.assetPairId) == null) {
            order.status = UnknownAsset.name
            marketOrderDatabaseAccessor.addMarketOrder(order)
            LOGGER.debug("Unknown asset: ${message.assetPairId}")
            messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder().setUid(message.uid).build())
            METRICS_LOGGER.log(getMetricLine(message.uid.toString(), order))
            return
        }

        val orderBook = limitOrderService.getOrderBook(order.assetPairId)?.getOrderBook(!order.isBuySide())
        if (orderBook == null || orderBook.size == 0) {
            order.status = NoLiquidity.name
            marketOrderDatabaseAccessor.addMarketOrder(order)
            if (orderBook == null) {
                LOGGER.debug("No liquidity, empty order book, for market order id: ${order.getId()}}, client: ${order.clientId}, asset: ${order.assetPairId}, volume: ${order.volume}, straight: ${order.straight}")
            } else {
                LOGGER.debug("No liquidity, no orders in order book, for market order id: ${order.getId()}}, client: ${order.clientId}, asset: ${order.assetPairId}, volume: ${order.volume}, straight: ${order.straight}")
            }
            messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder().setUid(message.uid).setRecordId(order.getId()).build())
            return
        }

        match(order, orderBook)
        messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder().setUid(message.uid).build())
        METRICS_LOGGER.log(getMetricLine(message.uid.toString(), order))
    }

    private fun parse(array: ByteArray): ProtocolMessages.MarketOrder {
        return ProtocolMessages.MarketOrder.parseFrom(array)
    }

    private fun getCrossVolume(volume: Double, straight: Boolean, price: Double): Double {
        return if (straight) volume else volume / price
    }

    private fun getVolume(volume: Double, straight: Boolean, price: Double): Double {
        return if (straight) volume else volume * price
    }

    private fun match(marketOrder: MarketOrder, orderBook: PriorityBlockingQueue<LimitOrder>) {
        var remainingVolume = marketOrder.getAbsVolume()
        val matchedOrders = LinkedList<LimitOrder>()
        val skipLimitOrders = HashSet<LimitOrder>()
        val cancelledLimitOrders = HashSet<LimitOrder>()

        var totalLimitPrice = 0.0
        var totalMarketVolume = 0.0
        while (remainingVolume.greaterThan(0.0) && orderBook.size > 0) {
            val limitOrder = orderBook.poll()
            val limitRemainingVolume = limitOrder.getAbsRemainingVolume()
            val marketRemainingVolume = getCrossVolume(remainingVolume, marketOrder.straight, limitOrder.price)
            val volume = if (marketRemainingVolume >= limitRemainingVolume) limitRemainingVolume else marketRemainingVolume
            if (marketOrder.clientId == limitOrder.clientId) {
                skipLimitOrders.add(limitOrder)
            } else if (limitOrderService.isEnoughFunds(limitOrder, volume)) {
                matchedOrders.add(limitOrder)
                remainingVolume -= getVolume(volume, marketOrder.straight, limitOrder.price)
                totalMarketVolume += volume
                totalLimitPrice += volume * limitOrder.price
            } else {
                cancelledLimitOrders.add(limitOrder)
            }
        }

        if (remainingVolume.greaterThan(0.0)) {
            marketOrder.status = NoLiquidity.name
            marketOrderDatabaseAccessor.addMarketOrder(marketOrder)
            matchedOrders.forEach { limitOrderService.addToOrderBook(it) }
            cancelledLimitOrders.forEach { limitOrderService.addToOrderBook(it) }
            skipLimitOrders.forEach { limitOrderService.addToOrderBook(it) }
            LOGGER.debug("No liquidity, not enough funds on limit orders, for market order id: ${marketOrder.getId()}}, client: ${marketOrder.clientId}, asset: ${marketOrder.assetPairId}, volume: ${marketOrder.volume} | Unfilled: $remainingVolume")
            return
        }

        if (!isEnoughFunds(marketOrder, if(marketOrder.isBuySide()) totalLimitPrice else totalMarketVolume )) {
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

        matchedOrders.forEach { limitOrder ->
            val limitRemainingVolume = limitOrder.getAbsRemainingVolume()
            val marketRemainingVolume = getCrossVolume(remainingVolume, marketOrder.straight, limitOrder.price)
            val volume = if (marketRemainingVolume >= limitRemainingVolume) limitRemainingVolume else marketRemainingVolume
            matchingData.add(MatchingData(marketOrder.getId(), limitOrder.getId(), volume))
            matchingData.add(MatchingData(limitOrder.getId(), marketOrder.getId(), volume))
            val isMarketBuy = marketOrder.isBuySide()
            val oppositeSideVolume = limitOrder.price * volume

            val clientTradePairs = ArrayList<ClientTradePair>()

            var uid = UUID.randomUUID().toString()
            marketTrades.add(Trade(marketOrder.clientId, uid,
                    assetPair.baseAssetId!!, now, limitOrder.getId(),
                    marketOrder.getId(), if (isMarketBuy) volume else -volume, limitOrder.price))
            orderTradesLinks.add(OrderTradesLink(marketOrder.getId(), uid))
            orderTradesLinks.add(OrderTradesLink(limitOrder.getId(), uid))
            cashMovements.add(WalletOperation(marketOrder.clientId, UUID.randomUUID().toString(),
                    assetPair.baseAssetId!!, now, if (isMarketBuy) volume else -volume))
            clientTradePairs.add(ClientTradePair(marketOrder.clientId, uid))

            uid = UUID.randomUUID().toString()
            marketTrades.add(Trade(marketOrder.clientId, uid,
                    assetPair.quotingAssetId!!, now, limitOrder.getId(),
                    marketOrder.getId(), if (isMarketBuy) -oppositeSideVolume else oppositeSideVolume, limitOrder.price))
            orderTradesLinks.add(OrderTradesLink(marketOrder.getId(), uid))
            orderTradesLinks.add(OrderTradesLink(limitOrder.getId(), uid))
            cashMovements.add(WalletOperation(marketOrder.clientId, UUID.randomUUID().toString(),
                    assetPair.quotingAssetId!!, now, if (isMarketBuy) -oppositeSideVolume else oppositeSideVolume))
            clientTradePairs.add(ClientTradePair(marketOrder.clientId, uid))

            uid = UUID.randomUUID().toString()
            limitTrades.add(Trade(limitOrder.clientId, uid,
                    assetPair.baseAssetId!!, now, limitOrder.getId(),
                    marketOrder.getId(), if (isMarketBuy) -volume else volume, limitOrder.price))
            orderTradesLinks.add(OrderTradesLink(marketOrder.getId(), uid))
            orderTradesLinks.add(OrderTradesLink(limitOrder.getId(), uid))
            cashMovements.add(WalletOperation(limitOrder.clientId, UUID.randomUUID().toString(),
                    assetPair.baseAssetId!!, now, if (isMarketBuy) -volume else volume))
            clientTradePairs.add(ClientTradePair(limitOrder.clientId, uid))

            uid = UUID.randomUUID().toString()
            limitTrades.add(Trade(limitOrder.clientId, uid,
                    assetPair.quotingAssetId!!, now, limitOrder.getId(),
                    marketOrder.getId(), if (isMarketBuy) oppositeSideVolume else -oppositeSideVolume, limitOrder.price))
            orderTradesLinks.add(OrderTradesLink(marketOrder.getId(), uid))
            orderTradesLinks.add(OrderTradesLink(limitOrder.getId(), uid))
            cashMovements.add(WalletOperation(limitOrder.clientId, UUID.randomUUID().toString(),
                    assetPair.quotingAssetId!!, now, if (isMarketBuy) oppositeSideVolume else -oppositeSideVolume))
            clientTradePairs.add(ClientTradePair(limitOrder.clientId, uid))

            if (marketRemainingVolume >= limitRemainingVolume) {
                limitOrder.remainingVolume = 0.0
                limitOrder.status = Matched.name
                completedLimitOrders.add(limitOrder)
            } else {
                limitOrder.remainingVolume -= if (limitOrder.isBuySide()) volume else -volume
                limitOrder.status = Processing.name
                uncompletedLimitOrder = limitOrder
            }
            remainingVolume -= getVolume(volume, marketOrder.straight, limitOrder.price)
            val transactionId = UUID.randomUUID().toString()
            limitOrder.lastMatchTime = now
            limitOrder.addTransactionIds(listOf(transactionId))
            transactionIds.add(transactionId)

            bitcoinTransactions.add(Swap(TransactionId = transactionId,
                                         clientId1 = marketOrder.clientId, Amount1 = if (isMarketBuy) oppositeSideVolume else volume, origAsset1 = if (isMarketBuy) assetPair.quotingAssetId!! else assetPair.baseAssetId!!,
                                         clientId2 = limitOrder.clientId, Amount2 = if (isMarketBuy) volume else oppositeSideVolume, origAsset2 = if (isMarketBuy) assetPair.baseAssetId!! else assetPair.quotingAssetId!!,
                                         orders = Orders(ClientOrderPair(marketOrder.clientId, marketOrder.getId()), ClientOrderPair(limitOrder.clientId, limitOrder.getId()),
                                                 clientTradePairs.toTypedArray())))
        }

        marketOrder.status = Matched.name
        marketOrder.matchedAt = now
        marketOrder.price = if (marketOrder.straight) totalLimitPrice / marketOrder.getAbsVolume() else marketOrder.getAbsVolume() / totalMarketVolume
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

        skipLimitOrders.forEach { limitOrderService.addToOrderBook(it) }

        if (uncompletedLimitOrder != null) {
            limitOrderService.updateLimitOrder(uncompletedLimitOrder as LimitOrder)
            limitOrderService.addToOrderBook(uncompletedLimitOrder as LimitOrder)
        }

        bitcoinTransactions.forEach { backendQueue.put(it) }

        LOGGER.debug("Market order id: ${marketOrder.getId()}}, client: ${marketOrder.clientId}, asset: ${marketOrder.assetPairId}, volume: ${marketOrder.volume}, straight: ${marketOrder.straight} matched, price: ${marketOrder.price}")
    }

    fun isEnoughFunds(order: MarketOrder, totalPrice: Double): Boolean {
        val assetPair = cashOperationService.getAssetPair(order.assetPairId)

        if (assetPair == null) {
            LOGGER.error("Unknown asset pair: ${order.assetPairId}")
            METRICS_LOGGER.logError(this.javaClass.name, "Unknown asset pair: ${order.assetPairId}")
            return false
        }
        val asset = if (order.isBuySide()) assetPair.quotingAssetId!! else assetPair.baseAssetId!!

        LOGGER.debug("${order.clientId} $asset : ${cashOperationService.getBalance(order.clientId, asset)} >= $totalPrice")
        return cashOperationService.getBalance(order.clientId, asset) >= totalPrice
    }

    fun getMetricLine(uid: String, order: MarketOrder): Line {
        return Line(ME_MARKET_ORDER, arrayOf(
                KeyValue(UID, uid),
                KeyValue(ID, order.id),
                KeyValue(TIMESTAMP, LocalDateTime.now().format(MetricsLogger.DATE_TIME_FORMATTER)),
                KeyValue(CLIENT_ID, order.clientId),
                KeyValue(ASSET_PAIR, order.assetPairId),
                KeyValue(AMOUNT, order.volume.toString()),
                KeyValue(STRAIGHT, order.straight.toString()),
                KeyValue(STATUS, order.status)
        ))
    }
}
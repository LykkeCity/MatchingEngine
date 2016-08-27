package com.lykke.matching.engine.services

import com.lykke.matching.engine.cache.WalletCredentialsCache
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.MarketOrder
import com.lykke.matching.engine.daos.MatchingData
import com.lykke.matching.engine.daos.OrderTradesLink
import com.lykke.matching.engine.daos.Trade
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.daos.bitcoin.ClientOrderPair
import com.lykke.matching.engine.daos.bitcoin.ClientTradePair
import com.lykke.matching.engine.daos.bitcoin.Orders
import com.lykke.matching.engine.database.BackOfficeDatabaseAccessor
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
import com.lykke.matching.engine.utils.RoundingUtils
import org.apache.log4j.Logger
import java.time.LocalDateTime
import java.util.ArrayList
import java.util.Date
import java.util.HashSet
import java.util.LinkedList
import java.util.UUID
import java.util.concurrent.BlockingQueue
import java.util.concurrent.PriorityBlockingQueue

class MarketOrderService(private val backOfficeDatabaseAccessor: BackOfficeDatabaseAccessor,
                         private val marketOrderDatabaseAccessor: MarketOrderDatabaseAccessor,
                         private val genericLimitOrderService: GenericLimitOrderService,
                         private val cashOperationService: CashOperationService,
                         private val backendQueue: BlockingQueue<Transaction>,
                         private val walletCredentialsCache: WalletCredentialsCache): AbsractService<ProtocolMessages.MarketOrder> {

    companion object {
        val LOGGER = Logger.getLogger(MarketOrderService::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()

        private val ORDER_ID = "OrderId"
    }

    private var messagesCount: Long = 0

    override fun processMessage(messageWrapper: MessageWrapper) {
        val message = parse(messageWrapper.byteArray)
        LOGGER.debug("Got market order id: ${message.uid}, client: ${message.clientId}, asset: ${message.assetPairId}, volume: ${RoundingUtils.roundForPrint(message.volume)}, straight: ${message.straight}")

        val order = MarketOrder(UUID.randomUUID().toString(), message.assetPairId, message.clientId, message.volume, null,
                Processing.name, Date(message.timestamp), Date(), null, null, message.straight)

        if (cashOperationService.getAssetPair(message.assetPairId) == null) {
            order.status = UnknownAsset.name
            marketOrderDatabaseAccessor.addMarketOrder(order)
            LOGGER.debug("Unknown asset: ${message.assetPairId}")
            messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder().setUid(message.uid).setRecordId(order.id).build())
            METRICS_LOGGER.log(getMetricLine(message.uid.toString(), order))
            METRICS_LOGGER.log(KeyValue(ME_MARKET_ORDER, (++messagesCount).toString()))
            return
        }

        val orderBook = genericLimitOrderService.getOrderBook(order.assetPairId)?.getOrderBook(!order.isBuySide)
        if (orderBook == null || orderBook.size == 0) {
            order.status = NoLiquidity.name
            marketOrderDatabaseAccessor.addMarketOrder(order)
            if (orderBook == null) {
                LOGGER.debug("No liquidity, empty order book, for market order id: ${order.id}}, client: ${order.clientId}, asset: ${order.assetPairId}, volume: ${RoundingUtils.roundForPrint(order.volume)}, straight: ${order.straight}")
            } else {
                LOGGER.debug("No liquidity, no orders in order book, for market order id: ${order.id}}, client: ${order.clientId}, asset: ${order.assetPairId}, volume: ${RoundingUtils.roundForPrint(order.volume)}, straight: ${order.straight}")
            }
            messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder().setUid(message.uid).setRecordId(order.id).build())
            return
        }

        match(order, orderBook)
        messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder().setUid(message.uid).setRecordId(order.id).build())
        METRICS_LOGGER.log(getMetricLine(message.uid.toString(), order))
        METRICS_LOGGER.log(KeyValue(ME_MARKET_ORDER, (++messagesCount).toString()))
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
        var remainingVolume = marketOrder.absVolume
        val matchedOrders = LinkedList<LimitOrder>()
        val skipLimitOrders = HashSet<LimitOrder>()
        val cancelledLimitOrders = HashSet<LimitOrder>()

        var totalLimitPrice = 0.0
        var totalMarketVolume = 0.0
        while (remainingVolume.greaterThan(0.0) && orderBook.size > 0) {
            val limitOrder = orderBook.poll()
            val limitRemainingVolume = limitOrder.absRemainingVolume
            val marketRemainingVolume = getCrossVolume(remainingVolume, marketOrder.straight, limitOrder.price)
            val volume = if (marketRemainingVolume >= limitRemainingVolume) limitRemainingVolume else marketRemainingVolume
            if (marketOrder.clientId == limitOrder.clientId) {
                skipLimitOrders.add(limitOrder)
            } else if (genericLimitOrderService.isEnoughFunds(limitOrder, volume)) {
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
            matchedOrders.forEach { genericLimitOrderService.addToOrderBook(it) }
            cancelledLimitOrders.forEach { genericLimitOrderService.addToOrderBook(it) }
            skipLimitOrders.forEach { genericLimitOrderService.addToOrderBook(it) }
            LOGGER.debug("No liquidity, not enough funds on limit orders, for market order id: ${marketOrder.id}}, client: ${marketOrder.clientId}, asset: ${marketOrder.assetPairId}, volume: ${RoundingUtils.roundForPrint(marketOrder.volume)} | Unfilled: $remainingVolume")
            return
        }

        if (!isEnoughFunds(marketOrder, if(marketOrder.isBuySide) totalLimitPrice else totalMarketVolume )) {
            marketOrder.status = NotEnoughFunds.name
            marketOrderDatabaseAccessor.addMarketOrder(marketOrder)
            matchedOrders.forEach { genericLimitOrderService.addToOrderBook(it) }
            cancelledLimitOrders.forEach { genericLimitOrderService.addToOrderBook(it) }
            skipLimitOrders.forEach { genericLimitOrderService.addToOrderBook(it) }
            LOGGER.debug("Not enough funds for market order id: ${marketOrder.id}}, client: ${marketOrder.clientId}, asset: ${marketOrder.assetPairId}, volume: ${RoundingUtils.roundForPrint(marketOrder.volume)}")
            return
        }

        remainingVolume = marketOrder.absVolume
        val now = Date()
        val assetPair = cashOperationService.getAssetPair(marketOrder.assetPairId)!!

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
            val limitRemainingVolume = limitOrder.absRemainingVolume
            val marketRemainingVolume = getCrossVolume(remainingVolume, marketOrder.straight, limitOrder.price)
            val volume = if (marketRemainingVolume >= limitRemainingVolume) limitRemainingVolume else marketRemainingVolume
            matchingData.add(MatchingData(marketOrder.id, limitOrder.id, volume))
            matchingData.add(MatchingData(limitOrder.id, marketOrder.id, volume))
            val isMarketBuy = marketOrder.isBuySide
            val oppositeSideVolume = limitOrder.price * volume

            val clientTradePairs = ArrayList<ClientTradePair>()

            val marketMultisig = walletCredentialsCache.getWalletCredentials(marketOrder.clientId)!!.multiSig
            val limitMultisig = walletCredentialsCache.getWalletCredentials(limitOrder.clientId)!!.multiSig

            val marketRoundedVolume = RoundingUtils.round(if (isMarketBuy) volume else -volume, cashOperationService.getAsset(assetPair.baseAssetId!!).accuracy, marketOrder.isOrigBuySide)
            var uid = Trade.generateId(now)
            var trade = Trade(marketOrder.clientId, uid, marketOrder.clientId, marketMultisig,
                    assetPair.baseAssetId!!, now, limitOrder.id,
                    marketOrder.id, marketRoundedVolume, limitOrder.price,
                    if (marketRoundedVolume > 0.0) limitMultisig else marketMultisig,
                    if (marketRoundedVolume > 0.0) marketMultisig else limitMultisig)
            marketTrades.add(trade)
            marketTrades.add(trade.cloneWithGeneratedId())
            marketTrades.add(trade.cloneWithMultisig())
            orderTradesLinks.add(OrderTradesLink(marketOrder.id, uid))
            orderTradesLinks.add(OrderTradesLink(limitOrder.id, uid))
            cashMovements.add(WalletOperation(marketOrder.clientId, UUID.randomUUID().toString(),
                    assetPair.baseAssetId!!, now, marketRoundedVolume))
            clientTradePairs.add(ClientTradePair(marketOrder.clientId, uid))

            val oppositeRoundedVolume = RoundingUtils.round(if (isMarketBuy) -oppositeSideVolume else oppositeSideVolume, cashOperationService.getAsset(assetPair.quotingAssetId!!).accuracy, marketOrder.isOrigBuySide)
            uid = Trade.generateId(now)
            trade = Trade(marketOrder.clientId, uid, marketOrder.clientId, marketMultisig,
                    assetPair.quotingAssetId!!, now, limitOrder.id,
                    marketOrder.id, oppositeRoundedVolume, limitOrder.price,
                    if (oppositeRoundedVolume > 0.0) limitMultisig else marketMultisig,
                    if (oppositeRoundedVolume > 0.0) marketMultisig else limitMultisig)
            marketTrades.add(trade)
            marketTrades.add(trade.cloneWithGeneratedId())
            marketTrades.add(trade.cloneWithMultisig())
            orderTradesLinks.add(OrderTradesLink(marketOrder.id, uid))
            orderTradesLinks.add(OrderTradesLink(limitOrder.id, uid))
            cashMovements.add(WalletOperation(marketOrder.clientId, UUID.randomUUID().toString(),
                    assetPair.quotingAssetId!!, now, oppositeRoundedVolume))
            clientTradePairs.add(ClientTradePair(marketOrder.clientId, uid))

            uid = Trade.generateId(now)
            trade = Trade(limitOrder.clientId, uid, limitOrder.clientId, limitMultisig,
                    assetPair.baseAssetId!!, now, limitOrder.id,
                    marketOrder.id, -marketRoundedVolume, limitOrder.price,
                    if (-marketRoundedVolume > 0.0) marketMultisig else limitMultisig,
                    if (-marketRoundedVolume > 0.0) limitMultisig else marketMultisig)
            limitTrades.add(trade)
            limitTrades.add(trade.cloneWithGeneratedId())
            limitTrades.add(trade.cloneWithMultisig())
            orderTradesLinks.add(OrderTradesLink(marketOrder.id, uid))
            orderTradesLinks.add(OrderTradesLink(limitOrder.id, uid))
            cashMovements.add(WalletOperation(limitOrder.clientId, UUID.randomUUID().toString(),
                    assetPair.baseAssetId!!, now, -marketRoundedVolume))
            clientTradePairs.add(ClientTradePair(limitOrder.clientId, uid))

            uid = Trade.generateId(now)
            trade = Trade(limitOrder.clientId, uid, limitOrder.clientId, limitMultisig,
                    assetPair.quotingAssetId!!, now, limitOrder.id,
                    marketOrder.id, -oppositeRoundedVolume, limitOrder.price,
                    if (-oppositeRoundedVolume > 0.0) marketMultisig else limitMultisig,
                    if (-oppositeRoundedVolume > 0.0) limitMultisig else marketMultisig)
            limitTrades.add(trade)
            limitTrades.add(trade.cloneWithGeneratedId())
            limitTrades.add(trade.cloneWithMultisig())
            orderTradesLinks.add(OrderTradesLink(marketOrder.id, uid))
            orderTradesLinks.add(OrderTradesLink(limitOrder.id, uid))
            cashMovements.add(WalletOperation(limitOrder.clientId, UUID.randomUUID().toString(),
                    assetPair.quotingAssetId!!, now, -oppositeRoundedVolume))
            clientTradePairs.add(ClientTradePair(limitOrder.clientId, uid))

            if (marketRemainingVolume >= limitRemainingVolume) {
                limitOrder.remainingVolume = 0.0
                limitOrder.status = Matched.name
                completedLimitOrders.add(limitOrder)
            } else {
                limitOrder.remainingVolume -= RoundingUtils.round(if (limitOrder.isBuySide) volume else -volume, cashOperationService.getAssetPair(limitOrder.assetPairId)!!.accuracy, marketOrder.isOrigBuySide)
                limitOrder.status = Processing.name
                uncompletedLimitOrder = limitOrder
            }
            remainingVolume -= getVolume(volume, marketOrder.straight, limitOrder.price)
            val transactionId = UUID.randomUUID().toString()
            limitOrder.lastMatchTime = now
            limitOrder.addTransactionIds(listOf(transactionId))
            transactionIds.add(transactionId)

            val asset1 = cashOperationService.getAsset(if (isMarketBuy) assetPair.quotingAssetId!! else assetPair.baseAssetId!!)
            val asset2 = cashOperationService.getAsset(if (isMarketBuy) assetPair.baseAssetId!! else assetPair.quotingAssetId!!)

            bitcoinTransactions.add(Swap(TransactionId = transactionId,
                                         clientId1 = marketOrder.clientId, Amount1 = RoundingUtils.round(if (isMarketBuy) oppositeSideVolume else volume, asset1.accuracy, marketOrder.isOrigBuySide), origAsset1 = asset1.assetId,
                                         clientId2 = limitOrder.clientId, Amount2 = RoundingUtils.round(if (isMarketBuy) volume else oppositeSideVolume, asset2.accuracy, marketOrder.isOrigBuySide), origAsset2 = asset2.assetId,
                                         orders = Orders(ClientOrderPair(marketOrder.clientId, marketOrder.id), ClientOrderPair(limitOrder.clientId, limitOrder.id),
                                                 clientTradePairs.toTypedArray())))
        }

        marketOrder.status = Matched.name
        marketOrder.matchedAt = now
        marketOrder.price = RoundingUtils.round(if (marketOrder.straight) totalLimitPrice / marketOrder.absVolume else marketOrder.absVolume / totalMarketVolume
                , cashOperationService.getAssetPair(marketOrder.assetPairId)!!.accuracy, marketOrder.isOrigBuySide)
        marketOrder.partitionKey = ORDER_ID
        marketOrder.addTransactionIds(transactionIds)
        marketOrderDatabaseAccessor.addMarketOrder(marketOrder)
        marketOrderDatabaseAccessor.addMarketOrderWithGeneratedRowId(marketOrder)

        marketOrderDatabaseAccessor.addTrades(marketTrades)
        marketOrderDatabaseAccessor.addTrades(limitTrades)
        marketOrderDatabaseAccessor.addMatchingData(matchingData)
        marketOrderDatabaseAccessor.addOrderTradesLinks(orderTradesLinks)

        cashOperationService.processWalletOperations(cashMovements)

        genericLimitOrderService.moveOrdersToDone(completedLimitOrders)
        cancelledLimitOrders.forEach { limitOrder ->
            limitOrder.status = NotEnoughFunds.name
        }
        genericLimitOrderService.moveOrdersToDone(ArrayList<LimitOrder>(cancelledLimitOrders))

        skipLimitOrders.forEach { genericLimitOrderService.addToOrderBook(it) }

        if (uncompletedLimitOrder != null) {
            genericLimitOrderService.updateLimitOrder(uncompletedLimitOrder as LimitOrder)
            genericLimitOrderService.addToOrderBook(uncompletedLimitOrder as LimitOrder)
        }

        bitcoinTransactions.forEach { backendQueue.put(it) }

        LOGGER.debug("Market order id: ${marketOrder.id}}, client: ${marketOrder.clientId}, asset: ${marketOrder.assetPairId}, volume: ${RoundingUtils.roundForPrint(marketOrder.volume)}, straight: ${marketOrder.straight} matched, price: ${RoundingUtils.roundForPrint(marketOrder.price)}")
    }

    fun isEnoughFunds(order: MarketOrder, totalPrice: Double): Boolean {
        val assetPair = cashOperationService.getAssetPair(order.assetPairId)

        if (assetPair == null) {
            LOGGER.error("Unknown asset pair: ${order.assetPairId}")
            METRICS_LOGGER.logError(this.javaClass.name, "Unknown asset pair: ${order.assetPairId}")
            return false
        }
        val asset = if (order.isBuySide) assetPair.quotingAssetId!! else assetPair.baseAssetId!!
        val roundedPrice = RoundingUtils.round(totalPrice, cashOperationService.getAsset(asset).accuracy, true)

        LOGGER.debug("${order.clientId} $asset : ${cashOperationService.getBalance(order.clientId, asset)} >= ${RoundingUtils.roundForPrint(roundedPrice)}")
        return cashOperationService.getBalance(order.clientId, asset) >= roundedPrice
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
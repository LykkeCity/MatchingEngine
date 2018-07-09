package com.lykke.matching.engine.order.cancel

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.database.DictionariesDatabaseAccessor
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.order.GenericLimitOrderProcessorFactory
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.outgoing.messages.LimitOrderWithTrades
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.services.AssetOrderBook
import com.lykke.matching.engine.services.GenericLimitOrderService
import org.apache.log4j.Logger
import java.math.BigDecimal
import java.util.Date
import java.util.concurrent.BlockingQueue

class LimitOrdersCanceller(dictionariesDatabaseAccessor: DictionariesDatabaseAccessor,
                           assetsPairsHolder: AssetsPairsHolder,
                           balancesHolder: BalancesHolder,
                           private val genericLimitOrderService: GenericLimitOrderService,
                           genericLimitOrderProcessorFactory: GenericLimitOrderProcessorFactory,
                           trustedClientsLimitOrdersQueue: BlockingQueue<JsonSerializable>,
                           clientsLimitOrdersQueue: BlockingQueue<JsonSerializable>,
                           private val orderBookQueue: BlockingQueue<OrderBook>,
                           private val rabbitOrderBookQueue: BlockingQueue<JsonSerializable>,
                           private val date: Date,
                           LOGGER: Logger) :
        AbstractLimitOrdersCanceller<AssetOrderBook, LimitOrdersCancelResult>(dictionariesDatabaseAccessor,
                assetsPairsHolder,
                balancesHolder,
                genericLimitOrderService,
                trustedClientsLimitOrdersQueue,
                clientsLimitOrdersQueue,
                date) {

    private val genericLimitOrderProcessor = genericLimitOrderProcessorFactory.create(LOGGER)
    private val orderBooks = HashMap<String, OrderBook>()

    override fun getCancelResult(walletOperations: List<WalletOperation>, clientsOrdersWithTrades: List<LimitOrderWithTrades>, trustedClientsOrdersWithTrades: List<LimitOrderWithTrades>, assetOrderBooks: Map<String, AssetOrderBook>): LimitOrdersCancelResult {
        return LimitOrdersCancelResult(walletOperations,
                clientsOrdersWithTrades,
                trustedClientsOrdersWithTrades, assetOrderBooks, orderBooks.values.toList())
    }

    override fun apply(messageId: String,
                       processedMessage: ProcessedMessage?,
                       result: LimitOrdersCancelResult) {
        super.apply(messageId, processedMessage, result)
        sendNotification()
    }

    private fun sendNotification() {
        orderBooks.values.forEach { orderBook ->
            genericLimitOrderService.putTradeInfo(TradeInfo(orderBook.assetPair, orderBook.isBuy, orderBook.prices.firstOrNull()?.price
                    ?: BigDecimal.ZERO, date))
            orderBookQueue.put(orderBook)
            rabbitOrderBookQueue.put(orderBook)
        }
    }

    fun checkAndProcessStopOrders(messageId: String) {
        val assetPairs = HashSet(ordersToCancel.keys)
        if (assetPairs.isNotEmpty()) {
            assetPairs.forEach { assetPair ->
                genericLimitOrderProcessor.checkAndProcessStopOrder(messageId, assetPair.assetPairId, date)
            }
        }
    }

    override fun getOrderLimitVolume(order: LimitOrder): BigDecimal {
        return order.reservedLimitVolume
                ?: if (order.isBuySide()) order.getAbsRemainingVolume() * order.price else order.getAbsRemainingVolume()
    }

    override fun processChangedOrderBook(orderBookCopy: AssetOrderBook, isBuy: Boolean) {
        orderBooks["${orderBookCopy.assetPairId}|$isBuy"] = OrderBook(orderBookCopy.assetPairId, isBuy, date, orderBookCopy.getOrderBook(isBuy))
    }

}
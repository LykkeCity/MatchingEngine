package com.lykke.matching.engine.order.cancel

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.database.DictionariesDatabaseAccessor
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.order.GenericLimitOrderProcessorFactory
import com.lykke.matching.engine.outgoing.messages.LimitOrderWithTrades
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.services.AssetOrderBook
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.utils.NumberUtils
import org.apache.log4j.Logger
import java.math.BigDecimal
import java.util.*
import java.util.concurrent.BlockingQueue

class LimitOrdersCanceller(dictionariesDatabaseAccessor: DictionariesDatabaseAccessor,
                           assetsHolder: AssetsHolder,
                           assetsPairsHolder: AssetsPairsHolder,
                           balancesHolder: BalancesHolder,
                           private val genericLimitOrderService: GenericLimitOrderService,
                           genericLimitOrderProcessorFactory: GenericLimitOrderProcessorFactory,
                           private val orderBookQueue: BlockingQueue<OrderBook>,
                           private val rabbitOrderBookQueue: BlockingQueue<OrderBook>,
                           clientLimitOrdersQueue: BlockingQueue<LimitOrdersReport>,
                           trustedClientsLimitOrdersQueue: BlockingQueue<LimitOrdersReport>,
                           private val date: Date,
                           LOGGER: Logger) :
        AbstractLimitOrdersCanceller<AssetOrderBook, LimitOrdersCancelResult>(dictionariesDatabaseAccessor,
                assetsHolder,
                assetsPairsHolder,
                balancesHolder,
                genericLimitOrderService,
                clientLimitOrdersQueue,
                trustedClientsLimitOrdersQueue,
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
        ordersToCancel.stream()
                .map { it.assetPairId }
                .filter(Objects::nonNull)
                .forEach {
                    genericLimitOrderProcessor.checkAndProcessStopOrder(messageId, it, date)
                }
    }

    override fun getOrderLimitVolume(order: LimitOrder, limitAsset: Asset): BigDecimal {
        return order.reservedLimitVolume ?: if (order.isBuySide())
            NumberUtils.setScale(order.getAbsRemainingVolume() * order.price, limitAsset.accuracy, false)
        else
            order.getAbsRemainingVolume()
    }

    override fun processChangedOrderBook(orderBookCopy: AssetOrderBook, isBuy: Boolean) {
        orderBooks["${orderBookCopy.assetId}|$isBuy"] = OrderBook(orderBookCopy.assetId, isBuy, date, orderBookCopy.getOrderBook(isBuy))
    }

}
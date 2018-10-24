package com.lykke.matching.engine.order.cancel

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.MidPrice
import com.lykke.matching.engine.database.DictionariesDatabaseAccessor
import com.lykke.matching.engine.database.common.entity.MidPricePersistenceData
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.holders.*
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.order.GenericLimitOrderProcessorFactory
import com.lykke.matching.engine.outgoing.messages.LimitOrderWithTrades
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.outgoing.messages.v2.builders.EventFactory
import com.lykke.matching.engine.services.AssetOrderBook
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.services.GenericStopLimitOrderService
import com.lykke.matching.engine.services.MessageSender
import org.apache.log4j.Logger
import org.springframework.util.CollectionUtils
import java.util.Date
import java.util.concurrent.BlockingQueue
import kotlin.collections.ArrayList
import kotlin.collections.Collection
import kotlin.collections.List
import kotlin.collections.Map
import kotlin.collections.forEach
import kotlin.collections.isNotEmpty
import kotlin.collections.mutableListOf

class GenericLimitOrdersCanceller(dictionariesDatabaseAccessor: DictionariesDatabaseAccessor,
                                  assetsHolder: AssetsHolder,
                                  private val assetsPairsHolder: AssetsPairsHolder,
                                  private val balancesHolder: BalancesHolder,
                                  orderBookQueue: BlockingQueue<OrderBook>,
                                  rabbitOrderBookQueue: BlockingQueue<OrderBook>,
                                  private val clientLimitOrdersQueue: BlockingQueue<LimitOrdersReport>,
                                  private val trustedClientsLimitOrdersQueue: BlockingQueue<LimitOrdersReport>,
                                  genericLimitOrderService: GenericLimitOrderService,
                                  genericStopLimitOrderService: GenericStopLimitOrderService,
                                  genericLimitOrderProcessorFactory: GenericLimitOrderProcessorFactory,
                                  private val messageSequenceNumberHolder: MessageSequenceNumberHolder,
                                  private val messageSender: MessageSender,
                                  private val date: Date,
                                  private val midPriceHolder: MidPriceHolder,
                                  LOGGER: Logger,
                                  private val cancelAll: Boolean = false) {

    private val limitOrdersCanceller = LimitOrdersCanceller(dictionariesDatabaseAccessor,
            assetsHolder,
            assetsPairsHolder,
            balancesHolder,
            genericLimitOrderService,
            genericLimitOrderProcessorFactory,
            orderBookQueue,
            rabbitOrderBookQueue,
            date,
            LOGGER)

    private val stopLimitOrdersCanceller = StopLimitOrdersCanceller(dictionariesDatabaseAccessor,
            assetsHolder,
            assetsPairsHolder,
            balancesHolder,
            genericStopLimitOrderService,
            date)

    fun preProcessLimitOrders(orders: Collection<LimitOrder>): GenericLimitOrdersCanceller {
        limitOrdersCanceller.preProcess(orders)
        return this
    }

    fun preProcessStopLimitOrders(orders: Collection<LimitOrder>): GenericLimitOrdersCanceller {
        stopLimitOrdersCanceller.preProcess(orders)
        return this
    }

    fun preProcessLimitOrders(ordersToCancel: List<LimitOrder>,
                              ordersToRemove: List<LimitOrder>): GenericLimitOrdersCanceller {
        limitOrdersCanceller.preProcess(ordersToCancel, ordersToRemove)
        return this
    }

    fun preProcessStopLimitOrders(ordersToCancel: List<LimitOrder>,
                                  ordersToRemove: List<LimitOrder>): GenericLimitOrdersCanceller {
        stopLimitOrdersCanceller.preProcess(ordersToCancel, ordersToRemove)
        return this
    }

    fun processLimitOrders(): LimitOrdersCancelResult {
        return limitOrdersCanceller.process()
    }

    fun processStopLimitOrders(): StopLimitOrdersCancelResult {
        return stopLimitOrdersCanceller.process()
    }

    fun applyFull(operationId: String,
                  messageId: String,
                  processedMessage: ProcessedMessage?,
                  messageType: MessageType,
                  validateBalances: Boolean): Boolean {
        val limitOrdersCancelResult = processLimitOrders()
        val stopLimitOrdersResult = processStopLimitOrders()

        val walletProcessor = balancesHolder.createWalletProcessor(null, validateBalances)
        walletProcessor.preProcess(limitOrdersCancelResult.walletOperations)
        walletProcessor.preProcess(stopLimitOrdersResult.walletOperations)

        val limitOrdersWithTrades = mutableListOf<LimitOrderWithTrades>()
        limitOrdersWithTrades.addAll(stopLimitOrdersResult.clientsOrdersWithTrades)
        limitOrdersWithTrades.addAll(limitOrdersCancelResult.clientsOrdersWithTrades)

        val trustedClientsLimitOrdersWithTrades = mutableListOf<LimitOrderWithTrades>()
        trustedClientsLimitOrdersWithTrades.addAll(stopLimitOrdersResult.trustedClientsOrdersWithTrades)
        trustedClientsLimitOrdersWithTrades.addAll(limitOrdersCancelResult.trustedClientsOrdersWithTrades)

        var sequenceNumber: Long? = null
        var clientsSequenceNumber: Long? = null
        var trustedClientsSequenceNumber: Long? = null
        if (trustedClientsLimitOrdersWithTrades.isNotEmpty()) {
            trustedClientsSequenceNumber = messageSequenceNumberHolder.getNewValue()
            sequenceNumber = trustedClientsSequenceNumber
        }
        if (limitOrdersWithTrades.isNotEmpty()) {
            clientsSequenceNumber = messageSequenceNumberHolder.getNewValue()
            sequenceNumber = clientsSequenceNumber
        }

        val midPricePersistenceData = getMidPricePersistenceData(limitOrdersCancelResult.assetOrderBooks)
        val updated = walletProcessor.persistBalances(processedMessage,
                limitOrdersCanceller.getPersistenceData(),
                stopLimitOrdersCanceller.getPersistenceData(),
                sequenceNumber, midPricePersistenceData)

        if (!updated) {
            return false
        }

        if(!CollectionUtils.isEmpty(midPricePersistenceData.midPrices)) {
            midPricePersistenceData.midPrices!!.forEach {
                val assetPair = assetsPairsHolder.getAssetPairAllowNulls(it.assetPairId)
                if (assetPair != null) {
                    midPriceHolder.addMidPrice(assetPair, it.midPrice, date)
                }
            }
        }

        walletProcessor.apply().sendNotification(operationId, messageType.name, messageId)
        stopLimitOrdersCanceller.apply(messageId, processedMessage, stopLimitOrdersResult)
        limitOrdersCanceller.apply(messageId, processedMessage, limitOrdersCancelResult)

        if (trustedClientsLimitOrdersWithTrades.isNotEmpty()) {
            trustedClientsLimitOrdersQueue.put(LimitOrdersReport(messageId, trustedClientsLimitOrdersWithTrades))
            messageSender.sendTrustedClientsMessage(EventFactory.createTrustedClientsExecutionEvent(trustedClientsSequenceNumber!!,
                    messageId,
                    operationId,
                    date,
                    messageType,
                    trustedClientsLimitOrdersWithTrades))
        }

        if (limitOrdersWithTrades.isNotEmpty()) {
            clientLimitOrdersQueue.put(LimitOrdersReport(messageId, limitOrdersWithTrades))
            messageSender.sendMessage(EventFactory.createExecutionEvent(clientsSequenceNumber!!,
                    messageId,
                    operationId,
                    date,
                    messageType,
                    walletProcessor.getClientBalanceUpdates(),
                    limitOrdersWithTrades))
        }

        limitOrdersCanceller.checkAndProcessStopOrders(messageId)

        return true
    }

    private fun getMidPricePersistenceData(assetPairIdToAssetOrderBook: Map<String, AssetOrderBook>): MidPricePersistenceData {
        if (cancelAll) {
            return MidPricePersistenceData(midPrice = null, removeAll = true)
        }

        val result = ArrayList<MidPrice>()
        assetPairIdToAssetOrderBook.forEach { assetPairId, orderBook ->
            val midPrice = orderBook.getMidPrice()
            if (midPrice != null) {
                result.add(MidPrice(assetPairId, midPrice, date.time))
            }
        }

        return MidPricePersistenceData(result)
    }
}
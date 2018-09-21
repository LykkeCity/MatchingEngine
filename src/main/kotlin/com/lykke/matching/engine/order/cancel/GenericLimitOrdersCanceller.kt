package com.lykke.matching.engine.order.cancel

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.database.DictionariesDatabaseAccessor
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.holders.MessageSequenceNumberHolder
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.order.GenericLimitOrderProcessorFactory
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.services.GenericStopLimitOrderService
import com.lykke.matching.engine.services.MessageSender
import org.apache.log4j.Logger
import java.util.Date
import java.util.concurrent.BlockingQueue

class GenericLimitOrdersCanceller(dictionariesDatabaseAccessor: DictionariesDatabaseAccessor,
                                  assetsHolder: AssetsHolder,
                                  assetsPairsHolder: AssetsPairsHolder,
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
                                  LOGGER: Logger) {

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
        return CancelOrdersUtils.persistAndSendNotifications(this,
                operationId,
                messageId,
                processedMessage,
                messageType,
                validateBalances,
                balancesHolder,
                messageSequenceNumberHolder,
                clientLimitOrdersQueue,
                trustedClientsLimitOrdersQueue,
                messageSender,
                date)
    }

    fun getPersistenceData(): OrdersCancelPersistenceData {
        return OrdersCancelPersistenceData(limitOrdersCanceller.getPersistenceData(),
                stopLimitOrdersCanceller.getPersistenceData())
    }

    fun checkAndProcessStopOrders(messageId: String) {
        limitOrdersCanceller.checkAndProcessStopOrders(messageId)
    }

    fun apply(messageId: String,
              processedMessage: ProcessedMessage?,
              stopLimitOrdersResult: StopLimitOrdersCancelResult?,
              limitOrdersCancelResult: LimitOrdersCancelResult?) {
        stopLimitOrdersResult?.let { stopLimitOrdersCanceller.apply(messageId, processedMessage, stopLimitOrdersResult) }
        limitOrdersCancelResult?.let { limitOrdersCanceller.apply(messageId, processedMessage, limitOrdersCancelResult) }
    }
}
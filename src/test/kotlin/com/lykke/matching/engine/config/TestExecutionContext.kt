package com.lykke.matching.engine.config

import com.lykke.matching.engine.daos.LkkTrade
import com.lykke.matching.engine.database.PersistenceManager
import com.lykke.matching.engine.fee.FeeProcessor
import com.lykke.matching.engine.holders.ApplicationSettingsHolder
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.holders.MessageSequenceNumberHolder
import com.lykke.matching.engine.matching.MatchingEngine
import com.lykke.matching.engine.order.ExecutionDataApplyService
import com.lykke.matching.engine.order.ExecutionEventSender
import com.lykke.matching.engine.order.ExecutionPersistenceService
import com.lykke.matching.engine.order.process.GenericLimitOrdersProcessor
import com.lykke.matching.engine.order.process.LimitOrderProcessor
import com.lykke.matching.engine.order.process.PreviousLimitOrdersProcessor
import com.lykke.matching.engine.order.process.StopLimitOrderProcessor
import com.lykke.matching.engine.order.process.StopOrderBookProcessor
import com.lykke.matching.engine.order.process.common.LimitOrdersCancelExecutor
import com.lykke.matching.engine.order.process.common.LimitOrdersCancelExecutorImpl
import com.lykke.matching.engine.order.process.common.LimitOrdersCanceller
import com.lykke.matching.engine.order.process.common.LimitOrdersCancellerImpl
import com.lykke.matching.engine.order.process.common.MatchingResultHandlingHelper
import com.lykke.matching.engine.order.transaction.ExecutionContextFactory
import com.lykke.matching.engine.order.transaction.ExecutionEventsSequenceNumbersGenerator
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.outgoing.messages.MarketOrderWithTrades
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.services.GenericStopLimitOrderService
import com.lykke.matching.engine.services.MessageSender
import com.lykke.matching.engine.services.validators.business.LimitOrderBusinessValidator
import com.lykke.matching.engine.services.validators.business.StopOrderBusinessValidator
import com.lykke.matching.engine.services.validators.input.LimitOrderInputValidator
import com.nhaarman.mockito_kotlin.any
import org.mockito.Mock
import org.mockito.Mockito
import org.mockito.MockitoAnnotations
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.task.TaskExecutor
import java.util.concurrent.BlockingDeque
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.locks.ReentrantLock

@Configuration
open class TestExecutionContext {

    @Mock
    private lateinit var syncEventsQueue: BlockingQueue<ExecutionEventSender.RabbitEventData>

    init {
        MockitoAnnotations.initMocks(this)

        val queue = LinkedBlockingQueue<Any>()

        val lock = ReentrantLock()
        val condition = lock.newCondition()

        var prevEvent: Any? = null
        var curEvent: Any? = null
        var consumedEvent: Any? = null

        Mockito.`when`(syncEventsQueue.put(any())).thenAnswer { invocation ->
            lock.lock()
            val event = invocation.arguments[0]
            queue.put(event)
            while (consumedEvent != event) {
                condition.await()
            }
            lock.unlock()
        }
        Mockito.`when`(syncEventsQueue.take()).thenAnswer {
            prevEvent = curEvent
            lock.lock()
            consumedEvent = prevEvent
            condition.signal()
            lock.unlock()
            curEvent = queue.take()
            curEvent

        }
    }

    @Bean
    open fun matchingEngine(genericLimitOrderService: GenericLimitOrderService,
                            balancesHolder: BalancesHolder,
                            feeProcessor: FeeProcessor): MatchingEngine {
        return MatchingEngine(genericLimitOrderService,
                feeProcessor)
    }

    @Bean
    open fun executionContextFactory(balancesHolder: BalancesHolder,
                                     genericLimitOrderService: GenericLimitOrderService,
                                     genericStopLimitOrderService: GenericStopLimitOrderService,
                                     assetsHolder: AssetsHolder): ExecutionContextFactory {
        return ExecutionContextFactory(balancesHolder,
                genericLimitOrderService,
                genericStopLimitOrderService,
                assetsHolder)
    }

    @Bean
    open fun executionEventsSequenceNumbersGenerator(messageSequenceNumberHolder: MessageSequenceNumberHolder): ExecutionEventsSequenceNumbersGenerator {
        return ExecutionEventsSequenceNumbersGenerator(messageSequenceNumberHolder)
    }

    @Bean
    open fun executionPersistenceService(persistenceManager: PersistenceManager): ExecutionPersistenceService {
        return ExecutionPersistenceService(persistenceManager)
    }

    @Bean
    open fun executionEventSender(messageSender: MessageSender,
                                  clientLimitOrdersQueue: BlockingQueue<LimitOrdersReport>,
                                  trustedClientsLimitOrdersQueue: BlockingQueue<LimitOrdersReport>,
                                  rabbitSwapQueue: BlockingQueue<MarketOrderWithTrades>,
                                  lkkTradesQueue: BlockingQueue<List<LkkTrade>>,
                                  genericLimitOrderService: GenericLimitOrderService,
                                  orderBookQueue: BlockingQueue<OrderBook>,
                                  rabbitOrderBookQueue: BlockingQueue<OrderBook>,
                                  rabbitEventsDataQueue: BlockingDeque<ExecutionEventSender.RabbitEventData>,
                                  rabbitPublishersThreadPool: TaskExecutor): ExecutionEventSender {
        return ExecutionEventSender(messageSender,
                clientLimitOrdersQueue,
                trustedClientsLimitOrdersQueue,
                rabbitSwapQueue,
                lkkTradesQueue,
                genericLimitOrderService,
                orderBookQueue,
                rabbitOrderBookQueue,
                syncEventsQueue,
                rabbitPublishersThreadPool)
    }

    @Bean
    open fun executionDataApplyService(executionEventsSequenceNumbersGenerator: ExecutionEventsSequenceNumbersGenerator,
                                       executionPersistenceService: ExecutionPersistenceService,
                                       executionEventSender: ExecutionEventSender): ExecutionDataApplyService {
        return ExecutionDataApplyService(executionEventsSequenceNumbersGenerator,
                executionPersistenceService,
                executionEventSender)
    }

    @Bean
    open fun limitOrderProcessor(limitOrderInputValidator: LimitOrderInputValidator,
                                 limitOrderBusinessValidator: LimitOrderBusinessValidator,
                                 applicationSettingsHolder: ApplicationSettingsHolder,
                                 matchingEngine: MatchingEngine,
                                 matchingResultHandlingHelper: MatchingResultHandlingHelper): LimitOrderProcessor {
        return LimitOrderProcessor(limitOrderInputValidator,
                limitOrderBusinessValidator,
                applicationSettingsHolder,
                matchingEngine,
                matchingResultHandlingHelper)
    }

    @Bean
    open fun stopLimitOrdersProcessor(limitOrderInputValidator: LimitOrderInputValidator,
                                      stopOrderBusinessValidator: StopOrderBusinessValidator,
                                      applicationSettingsHolder: ApplicationSettingsHolder,
                                      limitOrderProcessor: LimitOrderProcessor): StopLimitOrderProcessor {
        return StopLimitOrderProcessor(limitOrderInputValidator,
                stopOrderBusinessValidator,
                applicationSettingsHolder,
                limitOrderProcessor)
    }

    @Bean
    open fun genericLimitOrdersProcessor(limitOrderProcessor: LimitOrderProcessor,
                                         stopLimitOrdersProcessor: StopLimitOrderProcessor): GenericLimitOrdersProcessor {
        return GenericLimitOrdersProcessor(limitOrderProcessor, stopLimitOrdersProcessor)
    }

    @Bean
    open fun stopOrderBookProcessor(limitOrderProcessor: LimitOrderProcessor,
                                    applicationSettingsHolder: ApplicationSettingsHolder): StopOrderBookProcessor {
        return StopOrderBookProcessor(limitOrderProcessor, applicationSettingsHolder)
    }

    @Bean
    fun matchingResultHandlingHelper(applicationSettingsHolder: ApplicationSettingsHolder): MatchingResultHandlingHelper {
        return MatchingResultHandlingHelper(applicationSettingsHolder)
    }

    @Bean
    open fun previousLimitOrdersProcessor(genericLimitOrderService: GenericLimitOrderService,
                                          genericStopLimitOrderService: GenericStopLimitOrderService,
                                          limitOrdersCanceller: LimitOrdersCanceller): PreviousLimitOrdersProcessor {
        return PreviousLimitOrdersProcessor(genericLimitOrderService, genericStopLimitOrderService, limitOrdersCanceller)
    }

    @Bean
    open fun limitOrdersCanceller(applicationSettingsHolder: ApplicationSettingsHolder): LimitOrdersCanceller {
        return LimitOrdersCancellerImpl(applicationSettingsHolder)
    }

    @Bean
    open fun limitOrdersCancelExecutor(assetsPairsHolder: AssetsPairsHolder,
                                       executionContextFactory: ExecutionContextFactory,
                                       limitOrdersCanceller: LimitOrdersCanceller,
                                       stopOrderBookProcessor: StopOrderBookProcessor,
                                       executionDataApplyService: ExecutionDataApplyService): LimitOrdersCancelExecutor {
        return LimitOrdersCancelExecutorImpl(assetsPairsHolder,
                executionContextFactory,
                limitOrdersCanceller,
                stopOrderBookProcessor,
                executionDataApplyService)
    }
}
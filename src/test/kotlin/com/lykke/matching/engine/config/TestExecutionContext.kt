package com.lykke.matching.engine.config

import com.lykke.matching.engine.balance.WalletOperationsProcessorFactory
import com.lykke.matching.engine.daos.LkkTrade
import com.lykke.matching.engine.database.PersistenceManager
import com.lykke.matching.engine.fee.FeeProcessor
import com.lykke.matching.engine.holders.ApplicationSettingsHolder
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.MessageSequenceNumberHolder
import com.lykke.matching.engine.holders.MidPriceHolder
import com.lykke.matching.engine.holders.PriceDeviationThresholdHolder
import com.lykke.matching.engine.holders.UUIDHolder
import com.lykke.matching.engine.matching.MatchingEngine
import com.lykke.matching.engine.order.ExecutionDataApplyService
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
import com.lykke.matching.engine.outgoing.messages.CashOperation
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.outgoing.messages.MarketOrderWithTrades
import com.lykke.matching.engine.outgoing.messages.CashTransferOperation
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.outgoing.messages.OutgoingEventData
import com.lykke.matching.engine.outgoing.senders.OutgoingEventProcessor
import com.lykke.matching.engine.outgoing.senders.SpecializedEventSender
import com.lykke.matching.engine.outgoing.senders.impl.specialized.CashInOutEventSender
import com.lykke.matching.engine.outgoing.senders.impl.specialized.CashInOutOldEventSender
import com.lykke.matching.engine.outgoing.senders.impl.OutgoingEventProcessorImpl
import com.lykke.matching.engine.outgoing.senders.impl.specialized.CashTransferOldEventSender
import com.lykke.matching.engine.outgoing.senders.impl.specialized.CashTransferEventSender
import com.lykke.matching.engine.outgoing.senders.impl.specialized.ExecutionEventSender
import com.lykke.matching.engine.outgoing.senders.impl.specialized.OldFormatExecutionEventSender
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.services.GenericStopLimitOrderService
import com.lykke.matching.engine.services.MessageSender
import com.lykke.matching.engine.services.validators.business.LimitOrderBusinessValidator
import com.lykke.matching.engine.services.validators.business.StopOrderBusinessValidator
import com.lykke.matching.engine.services.validators.input.LimitOrderInputValidator
import com.lykke.matching.engine.utils.initSyncQueue
import org.mockito.Mock
import org.mockito.MockitoAnnotations
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.task.TaskExecutor
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue

@Configuration
open class TestExecutionContext {

    @Mock
    private lateinit var outgoingEvents: LinkedBlockingQueue<OutgoingEventData>

    init {
        MockitoAnnotations.initMocks(this)
        initSyncQueue(outgoingEvents)
    }

    @Bean
    open fun matchingEngine(genericLimitOrderService: GenericLimitOrderService,
                            feeProcessor: FeeProcessor,
                            uuidHolder: UUIDHolder): MatchingEngine {
        return MatchingEngine(genericLimitOrderService,
                feeProcessor,
                uuidHolder)
    }

    @Bean
    open fun executionContextFactory(walletOperationsProcessorFactory: WalletOperationsProcessorFactory,
                                     genericLimitOrderService: GenericLimitOrderService,
                                     genericStopLimitOrderService: GenericStopLimitOrderService,
                                     midPriceHolder: MidPriceHolder,
                                     priceDeviationThresholdHolder: PriceDeviationThresholdHolder,
                                     assetsHolder: AssetsHolder): ExecutionContextFactory {
        return ExecutionContextFactory(walletOperationsProcessorFactory,
                genericLimitOrderService,
                midPriceHolder,
                genericStopLimitOrderService,
                assetsHolder,
                priceDeviationThresholdHolder)
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
    open fun outGoingEventProcessor(messageSendersByEventClass: Map<Class<*>, List<SpecializedEventSender>>,
                                    @Qualifier("rabbitPublishersThreadPool")
                                    rabbitPublishersThreadPool: TaskExecutor): OutgoingEventProcessor {
        return OutgoingEventProcessorImpl(outgoingEvents, messageSendersByEventClass, rabbitPublishersThreadPool)
    }

    @Bean
    open fun cashTransferOldSender(notificationQueue: BlockingQueue<CashTransferOperation>): SpecializedEventSender {
        return CashTransferOldEventSender(notificationQueue)
    }

    @Bean
    open fun cashTransferNewSender(messageSender: MessageSender): SpecializedEventSender {
        return CashTransferEventSender(messageSender)
    }

    @Bean
    open fun specializedExecutionEventSender(messageSender: MessageSender,
                                             lkkTradesQueue: BlockingQueue<List<LkkTrade>>,
                                             genericLimitOrderService: GenericLimitOrderService,
                                             orderBookQueue: BlockingQueue<OrderBook>,
                                             rabbitOrderBookQueue: BlockingQueue<OrderBook>): SpecializedEventSender {
        return ExecutionEventSender(messageSender, lkkTradesQueue, genericLimitOrderService, orderBookQueue, rabbitOrderBookQueue)
    }

    @Bean
    open fun specializedOldExecutionEventSender(clientLimitOrdersQueue: BlockingQueue<LimitOrdersReport>,
                                                trustedClientsLimitOrdersQueue: BlockingQueue<LimitOrdersReport>,
                                                rabbitSwapQueue: BlockingQueue<MarketOrderWithTrades>): SpecializedEventSender {
        return OldFormatExecutionEventSender(
                clientLimitOrdersQueue,
                trustedClientsLimitOrdersQueue,
                rabbitSwapQueue)
    }

    @Bean
    open fun specializedCashInOutEventSender(messageSender: MessageSender): SpecializedEventSender {
        return CashInOutEventSender(messageSender)
    }

    @Bean
    open fun specializedCashInOutOldEventSender(rabbitCashInOutQueue: BlockingQueue<CashOperation>): SpecializedEventSender {
        return CashInOutOldEventSender(rabbitCashInOutQueue)
    }

    @Bean
    open fun executionDataApplyService(executionEventsSequenceNumbersGenerator: ExecutionEventsSequenceNumbersGenerator,
                                       executionPersistenceService: ExecutionPersistenceService,
                                       outgoingEventProcessor: OutgoingEventProcessor): ExecutionDataApplyService {
        return ExecutionDataApplyService(executionEventsSequenceNumbersGenerator,
                executionPersistenceService,
                outgoingEventProcessor)
    }

    @Bean
    open fun limitOrderProcessor(limitOrderInputValidator: LimitOrderInputValidator,
                                 limitOrderBusinessValidator: LimitOrderBusinessValidator,
                                 applicationSettingsHolder: ApplicationSettingsHolder,
                                 matchingEngine: MatchingEngine,
                                 priveDeviationThresholdHolder: PriceDeviationThresholdHolder,
                                 matchingResultHandlingHelper: MatchingResultHandlingHelper): LimitOrderProcessor {
        return LimitOrderProcessor(
                limitOrderBusinessValidator,
                applicationSettingsHolder,
                matchingEngine,
                matchingResultHandlingHelper)
    }

    @Bean
    open fun stopLimitOrdersProcessor(stopOrderBusinessValidator: StopOrderBusinessValidator,
                                      applicationSettingsHolder: ApplicationSettingsHolder,
                                      limitOrderProcessor: LimitOrderProcessor,
                                      uuidHolder: UUIDHolder): StopLimitOrderProcessor {
        return StopLimitOrderProcessor(
                stopOrderBusinessValidator,
                applicationSettingsHolder,
                limitOrderProcessor,
                uuidHolder)
    }

    @Bean
    open fun genericLimitOrdersProcessor(limitOrderProcessor: LimitOrderProcessor,
                                         stopLimitOrdersProcessor: StopLimitOrderProcessor): GenericLimitOrdersProcessor {
        return GenericLimitOrdersProcessor(limitOrderProcessor, stopLimitOrdersProcessor)
    }

    @Bean
    open fun stopOrderBookProcessor(limitOrderProcessor: LimitOrderProcessor,
                                    applicationSettingsHolder: ApplicationSettingsHolder,
                                    executionContextFactory: ExecutionContextFactory,
                                    priceDeviationThresholdHolder: PriceDeviationThresholdHolder,
                                    midPriceHolder: MidPriceHolder,
                                    uuidHolder: UUIDHolder): StopOrderBookProcessor {
        return StopOrderBookProcessor(limitOrderProcessor,
                applicationSettingsHolder,
                executionContextFactory,
                priceDeviationThresholdHolder,
                midPriceHolder,
                uuidHolder)
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
package com.lykke.matching.engine.config

import com.lykke.matching.engine.daos.LkkTrade
import com.lykke.matching.engine.database.PersistenceManager
import com.lykke.matching.engine.fee.FeeProcessor
import com.lykke.matching.engine.holders.ApplicationSettingsHolder
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.holders.MessageSequenceNumberHolder
import com.lykke.matching.engine.holders.UUIDHolder
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
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.util.concurrent.BlockingQueue

@Configuration
open class TestExecutionContext {

    @Bean
    open fun matchingEngine(genericLimitOrderService: GenericLimitOrderService,
                            feeProcessor: FeeProcessor,
                            uuidHolder: UUIDHolder): MatchingEngine {
        return MatchingEngine(genericLimitOrderService,
                feeProcessor,
                uuidHolder)
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
                                  rabbitOrderBookQueue: BlockingQueue<OrderBook>): ExecutionEventSender {
        return ExecutionEventSender(messageSender,
                clientLimitOrdersQueue,
                trustedClientsLimitOrdersQueue,
                rabbitSwapQueue,
                lkkTradesQueue,
                genericLimitOrderService,
                orderBookQueue,
                rabbitOrderBookQueue)
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
                                      limitOrderProcessor: LimitOrderProcessor,
                                      uuidHolder: UUIDHolder): StopLimitOrderProcessor {
        return StopLimitOrderProcessor(limitOrderInputValidator,
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
                                    uuidHolder: UUIDHolder): StopOrderBookProcessor {
        return StopOrderBookProcessor(limitOrderProcessor,
                applicationSettingsHolder,
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
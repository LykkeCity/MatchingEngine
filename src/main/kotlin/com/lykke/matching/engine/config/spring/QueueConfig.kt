package com.lykke.matching.engine.config.spring

import com.lykke.matching.engine.daos.LkkTrade
import com.lykke.matching.engine.daos.TransferOperation
import com.lykke.matching.engine.database.reconciliation.events.AccountPersistEvent
import com.lykke.matching.engine.database.reconciliation.events.OrderBookPersistEvent
import com.lykke.matching.engine.database.reconciliation.events.StopOrderBookPersistEvent
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.outgoing.messages.*
import com.lykke.matching.engine.outgoing.messages.v2.events.Event
import com.lykke.matching.engine.outgoing.messages.v2.events.ExecutionEvent
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.util.concurrent.BlockingDeque
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingDeque
import java.util.concurrent.LinkedBlockingQueue
import com.lykke.matching.engine.logging.MessageWrapper as LoggingMessageWrapper

@Configuration
open class QueueConfig {

    //<editor-fold desc="Rabbit MQ queues">
    @Bean
    @RabbitQueue
    open fun clientsEventsQueue(): BlockingDeque<Event<*>> {
        return LinkedBlockingDeque()
    }

    @Bean
    @RabbitQueue
    open fun trustedClientsEventsQueue(): BlockingDeque<ExecutionEvent> {
        return LinkedBlockingDeque()
    }

    @Bean
    @RabbitQueue
    open fun balanceUpdateQueue(): BlockingDeque<BalanceUpdate> {
        return LinkedBlockingDeque<BalanceUpdate>()
    }

    @Bean
    @RabbitQueue
    open fun clientLimitOrdersQueue(): BlockingDeque<LimitOrdersReport> {
        return LinkedBlockingDeque<LimitOrdersReport>()
    }

    @Bean
    @RabbitQueue
    open fun rabbitOrderBookQueue(): BlockingDeque<OrderBook> {
        return LinkedBlockingDeque<OrderBook>()
    }

    @Bean
    @RabbitQueue
    open fun rabbitCashInOutQueue(): BlockingDeque<CashOperation> {
        return LinkedBlockingDeque<CashOperation>()
    }

    @Bean
    @RabbitQueue
    open fun rabbitMarketOrderWithTradesQueue(): BlockingDeque<MarketOrderWithTrades> {
        return LinkedBlockingDeque<MarketOrderWithTrades>()
    }

    @Bean
    @RabbitQueue
    open fun rabbitTransferQueue(): BlockingDeque<CashTransferOperation> {
        return LinkedBlockingDeque<CashTransferOperation>()
    }

    @Bean
    @RabbitQueue
    open fun reservedCashOperationQueue(): BlockingDeque<ReservedCashOperation> {
        return LinkedBlockingDeque<ReservedCashOperation>()
    }

    @Bean
    @RabbitQueue
    open fun trustedClientsLimitOrdersQueue(): BlockingDeque<LimitOrdersReport> {
        return LinkedBlockingDeque<LimitOrdersReport>()
    }
    //</editor-fold>


    //<editor-fold desc="Input queues">
    @Bean
    @InputQueue
    open fun limitOrderInputQueue(): BlockingQueue<MessageWrapper> {
        return LinkedBlockingQueue<MessageWrapper>()
    }

    @Bean
    @InputQueue
    open fun cashInOutInputQueue(): BlockingQueue<MessageWrapper> {
        return LinkedBlockingQueue<MessageWrapper>()
    }

    @Bean
    @InputQueue
    open fun cashTransferInputQueue(): BlockingQueue<MessageWrapper> {
        return LinkedBlockingQueue<MessageWrapper>()
    }

    @Bean
    @InputQueue
    open fun limitOrderCancelInputQueue(): BlockingQueue<MessageWrapper>{
        return LinkedBlockingQueue<MessageWrapper>()
    }

    @Bean
    @InputQueue
    open fun limitOrderMassCancelInputQueue(): BlockingQueue<MessageWrapper>{
        return LinkedBlockingQueue<MessageWrapper>()
    }

    @Bean
    @InputQueue
    open fun preProcessedMessageQueue(): BlockingQueue<MessageWrapper> {
        return LinkedBlockingQueue<MessageWrapper>()
    }
    //</editor-fold>

    //<editor-fold desc="Data queues">
    @Bean
    @DataQueue
    open fun balanceUpdatesLogQueue(): BlockingQueue<LoggingMessageWrapper> {
        return LinkedBlockingQueue()
    }

    @Bean
    @DataQueue
    open fun cashInOutLogQueue(): BlockingQueue<LoggingMessageWrapper> {
        return LinkedBlockingQueue()
    }

    @Bean
    @DataQueue
    open fun cashTransferLogQueue(): BlockingQueue<LoggingMessageWrapper> {
        return LinkedBlockingQueue()
    }

    @Bean
    @DataQueue
    open fun clientLimitOrdersLogQueue(): BlockingQueue<LoggingMessageWrapper> {
        return LinkedBlockingQueue()
    }

    @Bean
    @DataQueue
    open fun marketOrderWithTradesLogQueue(): BlockingQueue<LoggingMessageWrapper> {
        return LinkedBlockingQueue()
    }

    @Bean
    @DataQueue
    open fun reservedCashOperationLogQueue(): BlockingQueue<LoggingMessageWrapper> {
        return LinkedBlockingQueue()
    }

    @Bean
    @DataQueue
    open fun updatedOrderBooksQueue(): BlockingQueue<OrderBookPersistEvent>? {
        return LinkedBlockingQueue<OrderBookPersistEvent>()
    }

    @Bean
    @DataQueue
    open fun updatedStopOrderBooksQueue(): BlockingQueue<StopOrderBookPersistEvent>? {
        return LinkedBlockingQueue<StopOrderBookPersistEvent>()
    }

    @Bean
    @DataQueue
    open fun updatedWalletsQueue(): BlockingQueue<AccountPersistEvent>? {
        return LinkedBlockingQueue<AccountPersistEvent>()
    }

    @Bean
    @DataQueue
    open fun lkkTradesQueue(): BlockingQueue<List<LkkTrade>> {
        return LinkedBlockingQueue<List<LkkTrade>>()
    }

    @Bean
    @DataQueue
    open fun orderBookQueue(): BlockingQueue<OrderBook> {
        return LinkedBlockingQueue<OrderBook>()
    }

    @Bean
    @DataQueue
    open fun dbTransferOperationQueue(): BlockingQueue<TransferOperation> {
        return LinkedBlockingQueue<TransferOperation>()
    }
    //</editor-fold>
}
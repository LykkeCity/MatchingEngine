package com.lykke.matching.engine.config.spring

import com.lykke.matching.engine.daos.LkkTrade
import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.daos.TransferOperation
import com.lykke.matching.engine.database.reconciliation.events.AccountPersistEvent
import com.lykke.matching.engine.database.reconciliation.events.OrderBookPersistEvent
import com.lykke.matching.engine.database.reconciliation.events.StopOrderBookPersistEvent
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.notification.BalanceUpdateNotification
import com.lykke.matching.engine.notification.QuotesUpdate
import com.lykke.matching.engine.outgoing.messages.*
import com.lykke.matching.engine.outgoing.messages.v2.events.Event
import com.lykke.matching.engine.outgoing.messages.v2.events.ExecutionEvent
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.util.concurrent.BlockingDeque
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingDeque
import java.util.concurrent.LinkedBlockingQueue

@Configuration
open class QueueConfig {

    //<editor-fold desc="Rabbit MQ queues">
    @Bean
    @RabbitQueue
    open fun clientsEventsQueue(): BlockingDeque<Event<*>> {
        return LinkedBlockingDeque()
    }

    @Bean
    open fun trustedClientsEventsQueue(): BlockingDeque<ExecutionEvent> {
        return LinkedBlockingDeque()
    }

    @Bean
    open fun balanceUpdateQueue(): BlockingDeque<BalanceUpdate> {
        return LinkedBlockingDeque<BalanceUpdate>()
    }

    @Bean
    open fun cashSwapQueue(): BlockingDeque<CashSwapOperation> {
        return LinkedBlockingDeque<CashSwapOperation>()
    }

    @Bean
    open fun clientLimitOrdersQueue(): BlockingDeque<LimitOrdersReport> {
        return LinkedBlockingDeque<LimitOrdersReport>()
    }

    @Bean
    open fun rabbitOrderBookQueue(): BlockingDeque<OrderBook> {
        return LinkedBlockingDeque<OrderBook>()
    }

    @Bean
    open fun rabbitCashInOutQueue(): BlockingDeque<CashOperation> {
        return LinkedBlockingDeque<CashOperation>()
    }

    @Bean
    open fun rabbitSwapQueue(): BlockingDeque<MarketOrderWithTrades> {
        return LinkedBlockingDeque<MarketOrderWithTrades>()
    }

    @Bean
    open fun rabbitTransferQueue(): BlockingDeque<CashTransferOperation> {
        return LinkedBlockingDeque<CashTransferOperation>()
    }

    @Bean
    open fun reservedCashOperationQueue(): BlockingDeque<ReservedCashOperation> {
        return LinkedBlockingDeque<ReservedCashOperation>()
    }

    @Bean
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


    //<editor-fold desc="Etc queues">
    @Bean
    open fun orderBookQueue(): BlockingQueue<OrderBook> {
        return LinkedBlockingQueue<OrderBook>()
    }

    @Bean
    open fun balanceUpdateNotificationQueue(): BlockingQueue<BalanceUpdateNotification> {
        return  LinkedBlockingQueue<BalanceUpdateNotification>()
    }

    @Bean
    open fun quotesUpdateQueue(): BlockingQueue<QuotesUpdate> {
        return LinkedBlockingQueue<QuotesUpdate>()
    }

    @Bean
    open fun tradeInfoQueue(): BlockingQueue<TradeInfo> {
        return LinkedBlockingQueue<TradeInfo>()
    }

    @Bean
    open fun dbTransferOperationQueue(): BlockingQueue<TransferOperation> {
        return LinkedBlockingQueue<TransferOperation>()
    }

    @Bean
    open fun lkkTradesQueue(): BlockingQueue<List<LkkTrade>> {
        return LinkedBlockingQueue<List<LkkTrade>>()
    }

    @Bean
    open fun updatedOrderBooksQueue(): BlockingQueue<OrderBookPersistEvent>? {
        return LinkedBlockingQueue<OrderBookPersistEvent>()
    }

    @Bean
    open fun updatedStopOrderBooksQueue(): BlockingQueue<StopOrderBookPersistEvent>? {
        return LinkedBlockingQueue<StopOrderBookPersistEvent>()
    }

    @Bean
    open fun updatedWalletsQueue(): BlockingQueue<AccountPersistEvent>? {
        return LinkedBlockingQueue<AccountPersistEvent>()
    }
    //</editor-fold>
}
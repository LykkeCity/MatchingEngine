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
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue

@Configuration
open class QueueConfig {

    @Bean
    @RabbitQueue
    open fun clientsEventsQueue(): BlockingQueue<Event<*>> {
        return LinkedBlockingQueue()
    }

    @Bean
    @RabbitQueue
    open fun trustedClientsEventsQueue(): BlockingQueue<ExecutionEvent> {
        return LinkedBlockingQueue()
    }

    @Bean
    @RabbitQueue
    open fun balanceUpdateQueue(): BlockingQueue<BalanceUpdate> {
        return LinkedBlockingQueue<BalanceUpdate>()
    }

    @Bean
    open fun balanceUpdateNotificationQueue(): BlockingQueue<BalanceUpdateNotification> {
        return  LinkedBlockingQueue<BalanceUpdateNotification>()
    }

    @Bean
    @RabbitQueue
    open fun cashSwapQueue(): BlockingQueue<CashSwapOperation> {
        return LinkedBlockingQueue<CashSwapOperation>()
    }

    @Bean
    @RabbitQueue
    open fun clientLimitOrdersQueue(): BlockingQueue<LimitOrdersReport> {
        return LinkedBlockingQueue<LimitOrdersReport>()
    }

    @Bean
    open fun lkkTradesQueue(): BlockingQueue<List<LkkTrade>> {
        return LinkedBlockingQueue<List<LkkTrade>>()
    }

    @Bean
    @RabbitQueue
    open fun rabbitOrderBookQueue(): BlockingQueue<OrderBook> {
        return LinkedBlockingQueue<OrderBook>()
    }

    @Bean
    @RabbitQueue
    open fun rabbitCashInOutQueue(): BlockingQueue<CashOperation> {
        return LinkedBlockingQueue<CashOperation>()
    }

    @Bean
    @RabbitQueue
    open fun rabbitSwapQueue(): BlockingQueue<MarketOrderWithTrades> {
        return LinkedBlockingQueue<MarketOrderWithTrades>()
    }

    @Bean
    @RabbitQueue
    open fun rabbitTransferQueue(): BlockingQueue<CashTransferOperation> {
        return LinkedBlockingQueue<CashTransferOperation>()
    }

    @Bean
    @RabbitQueue
    open fun reservedCashOperationQueue(): BlockingQueue<ReservedCashOperation> {
        return LinkedBlockingQueue<ReservedCashOperation>()
    }

    @Bean
    @RabbitQueue
    open fun trustedClientsLimitOrdersQueue(): BlockingQueue<LimitOrdersReport> {
        return LinkedBlockingQueue<LimitOrdersReport>()
    }

    @Bean
    open fun orderBookQueue(): BlockingQueue<OrderBook> {
        return LinkedBlockingQueue<OrderBook>()
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
}
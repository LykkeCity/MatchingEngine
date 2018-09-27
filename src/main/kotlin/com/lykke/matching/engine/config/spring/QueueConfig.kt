package com.lykke.matching.engine.config.spring

import com.lykke.matching.engine.daos.LkkTrade
import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.daos.TransferOperation
import com.lykke.matching.engine.daos.wallet.Wallet
import com.lykke.matching.engine.database.common.entity.OrderBookPersistenceData
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.outgoing.messages.*
import com.lykke.matching.engine.outgoing.messages.v2.events.Event
import com.lykke.matching.engine.outgoing.messages.v2.events.ExecutionEvent
import com.lykke.matching.engine.utils.config.Config
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue

@Configuration
open class QueueConfig {

    @Autowired
    private lateinit var config: Config

    @Bean
    open fun clientsEventsQueue(): BlockingQueue<Event<*>> {
        return LinkedBlockingQueue()
    }

    @Bean
    open fun trustedClientsEventsQueue(): BlockingQueue<ExecutionEvent> {
        return LinkedBlockingQueue()
    }

    @Bean
    open fun balanceUpdateQueue(): BlockingQueue<BalanceUpdate> {
        return LinkedBlockingQueue<BalanceUpdate>()
    }

    @Bean
    open fun clientLimitOrdersQueue(): BlockingQueue<LimitOrdersReport> {
        return LinkedBlockingQueue<LimitOrdersReport>()
    }

    @Bean
    open fun lkkTradesQueue(): BlockingQueue<List<LkkTrade>> {
        return LinkedBlockingQueue<List<LkkTrade>>()
    }

    @Bean
    open fun orderBookQueue(): BlockingQueue<OrderBook> {
        return LinkedBlockingQueue<OrderBook>()
    }

    @Bean
    open fun rabbitOrderBookQueue(): BlockingQueue<OrderBook> {
        return LinkedBlockingQueue<OrderBook>()
    }

    @Bean
    open fun rabbitCashInOutQueue(): BlockingQueue<CashOperation> {
        return LinkedBlockingQueue<CashOperation>()
    }

    @Bean
    open fun rabbitMarketOrderWithTradesQueue(): BlockingQueue<MarketOrderWithTrades> {
        return LinkedBlockingQueue<MarketOrderWithTrades>()
    }

    @Bean
    open fun rabbitTransferQueue(): BlockingQueue<CashTransferOperation> {
        return LinkedBlockingQueue<CashTransferOperation>()
    }

    @Bean
    open fun reservedCashOperationQueue(): BlockingQueue<ReservedCashOperation> {
        return LinkedBlockingQueue<ReservedCashOperation>()
    }

    @Bean
    open fun trustedClientsLimitOrdersQueue(): BlockingQueue<LimitOrdersReport> {
        return LinkedBlockingQueue<LimitOrdersReport>()
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
    open fun limitOrderInputQueue(): BlockingQueue<MessageWrapper> {
        return LinkedBlockingQueue<MessageWrapper>()
    }

    @Bean
    open fun preProcessedMessageQueue(): BlockingQueue<MessageWrapper> {
        return LinkedBlockingQueue<MessageWrapper>()
    }

    @Bean
    open fun limitOrderCancelInputQueue(): BlockingQueue<MessageWrapper>{
        return LinkedBlockingQueue<MessageWrapper>()
    }

    @Bean
    open fun limitOrderMassCancelInputQueue(): BlockingQueue<MessageWrapper>{
        return LinkedBlockingQueue<MessageWrapper>()
    }

    @Bean
    open fun cashInOutInputQueue(): BlockingQueue<MessageWrapper> {
        return LinkedBlockingQueue<MessageWrapper>()
    }

    @Bean
    open fun cashTransferInputQueue(): BlockingQueue<MessageWrapper> {
        return LinkedBlockingQueue<MessageWrapper>()
    }

    @Bean
    open fun updatedOrderBooksQueue(): BlockingQueue<Collection<OrderBookPersistenceData>>? {
        return LinkedBlockingQueue<Collection<OrderBookPersistenceData>>()
    }

    @Bean
    open fun updatedStopOrderBooksQueue(): BlockingQueue<Collection<OrderBookPersistenceData>>? {
        return LinkedBlockingQueue<Collection<OrderBookPersistenceData>>()
    }

    @Bean
    open fun updatedWalletsQueue(): BlockingQueue<Collection<Wallet>>? {
        return LinkedBlockingQueue<Collection<Wallet>>()
    }

}
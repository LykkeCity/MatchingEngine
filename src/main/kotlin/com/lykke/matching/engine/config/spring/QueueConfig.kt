package com.lykke.matching.engine.config.spring

import com.lykke.matching.engine.daos.LkkTrade
import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.notification.BalanceUpdateNotification
import com.lykke.matching.engine.notification.QuotesUpdate
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue

@Configuration
open class QueueConfig {

    @Bean
    open fun balanceUpdateQueue(): BlockingQueue<JsonSerializable> {
        return LinkedBlockingQueue<JsonSerializable>()
    }

    @Bean
    open fun balanceUpdateNotificationQueue(): BlockingQueue<BalanceUpdateNotification> {
        return  LinkedBlockingQueue<BalanceUpdateNotification>()
    }

    @Bean
    open fun cashSwapQueue(): BlockingQueue<JsonSerializable> {
        return LinkedBlockingQueue<JsonSerializable>()
    }

    @Bean
    open fun clientLimitOrdersQueue(): BlockingQueue<JsonSerializable> {
        return LinkedBlockingQueue<JsonSerializable>()
    }

    @Bean
    open fun lkkTradesQueue(): BlockingQueue<List<LkkTrade>> {
        return LinkedBlockingQueue<List<LkkTrade>>()
    }

    @Bean
    open fun orderBookQueue(): BlockingQueue<JsonSerializable> {
        return LinkedBlockingQueue<JsonSerializable>()
    }

    @Bean
    open fun rabbitOrderBookQueue(): BlockingQueue<JsonSerializable> {
        return LinkedBlockingQueue<JsonSerializable>()
    }

    @Bean
    open fun rabbitSwapQueue(): BlockingQueue<JsonSerializable> {
        return LinkedBlockingQueue<JsonSerializable>()
    }

    @Bean
    open fun reservedCashOperationQueue(): BlockingQueue<JsonSerializable> {
        return LinkedBlockingQueue<JsonSerializable>()
    }

    @Bean
    open fun trustedClientsLimitOrderQueue(): BlockingQueue<JsonSerializable> {
        return LinkedBlockingQueue<JsonSerializable>()
    }

    @Bean
    open fun quotesUpdateQueue(): BlockingQueue<QuotesUpdate> {
        return LinkedBlockingQueue<QuotesUpdate>()
    }

    @Bean
    open fun tradeInfoQueue(): BlockingQueue<TradeInfo> {
        return LinkedBlockingQueue<TradeInfo>()
    }
}
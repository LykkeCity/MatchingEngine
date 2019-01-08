package com.lykke.matching.engine.database.common.strategy

import com.lykke.matching.engine.common.SimpleApplicationEventPublisher
import com.lykke.matching.engine.database.Storage
import com.lykke.matching.engine.database.reconciliation.events.OrderBookPersistEvent
import com.lykke.matching.engine.database.reconciliation.events.StopOrderBookPersistEvent
import com.lykke.matching.engine.utils.config.Config
import org.springframework.beans.factory.FactoryBean
import org.springframework.stereotype.Component

@Component
class OrdersPersistInSecondaryDbStrategyFactory(private val persistedOrdersApplicationEventPublisher: SimpleApplicationEventPublisher<OrderBookPersistEvent>,
                                                private val persistedStopApplicationEventPublisher: SimpleApplicationEventPublisher<StopOrderBookPersistEvent>,
                                                private val config: Config) :FactoryBean<OrdersPersistInSecondaryDbStrategy> {
    override fun getObject(): OrdersPersistInSecondaryDbStrategy? {
        if(config.me.storage == Storage.Redis || config.me.storage == Storage.RedisWithoutOrders) {
            return AzureSecondaryDbOrderPersistStrategy(persistedOrdersApplicationEventPublisher, persistedStopApplicationEventPublisher)
        }

        return null
    }

    override fun getObjectType(): Class<*>? {
        return OrdersPersistInSecondaryDbStrategy::class.java
    }
}
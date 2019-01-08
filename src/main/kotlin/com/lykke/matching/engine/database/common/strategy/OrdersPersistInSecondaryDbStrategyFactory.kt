package com.lykke.matching.engine.database.common.strategy

import com.lykke.matching.engine.common.SimpleApplicationEventPublisher
import com.lykke.matching.engine.database.Storage
import com.lykke.matching.engine.database.reconciliation.events.OrderBookPersistEvent
import com.lykke.matching.engine.database.reconciliation.events.StopOrderBookPersistEvent
import com.lykke.matching.engine.utils.config.Config
import org.springframework.beans.factory.FactoryBean
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component

@Component
class OrdersPersistInSecondaryDbStrategyFactory() : FactoryBean<OrdersPersistInSecondaryDbStrategy> {

    @Autowired
    private lateinit var persistedOrdersApplicationEventPublisher: SimpleApplicationEventPublisher<OrderBookPersistEvent>

    @Autowired
    private lateinit var persistedStopApplicationEventPublisher: SimpleApplicationEventPublisher<StopOrderBookPersistEvent>

    @Autowired
    private lateinit var config: Config

    override fun getObject(): OrdersPersistInSecondaryDbStrategy? {
        if (config.me.storage == Storage.Redis || config.me.storage == Storage.RedisWithoutOrders) {
            return AzureSecondaryDbOrderPersistStrategy(persistedOrdersApplicationEventPublisher, persistedStopApplicationEventPublisher)
        }

        return null
    }

    override fun getObjectType(): Class<*>? {
        return OrdersPersistInSecondaryDbStrategy::class.java
    }
}
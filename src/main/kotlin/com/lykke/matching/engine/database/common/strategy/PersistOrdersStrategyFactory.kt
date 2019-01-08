package com.lykke.matching.engine.database.common.strategy

import com.lykke.matching.engine.database.Storage
import com.lykke.matching.engine.holders.OrdersDatabaseAccessorsHolder
import com.lykke.matching.engine.holders.StopOrdersDatabaseAccessorsHolder
import com.lykke.matching.engine.utils.config.Config
import org.springframework.beans.factory.FactoryBean
import org.springframework.stereotype.Component

@Component
class PersistOrdersStrategyFactory(private val config: Config,
                                   private val ordersDatabaseAccessorsHolder: OrdersDatabaseAccessorsHolder,
                                   private val stopOrdersDatabaseAccessorsHolder: StopOrdersDatabaseAccessorsHolder) : FactoryBean<PersistOrdersDuringRedisTransactionStrategy> {
    override fun getObjectType(): Class<*>? {
        return PersistOrdersDuringRedisTransactionStrategy::class.java
    }

    override fun getObject(): PersistOrdersDuringRedisTransactionStrategy? {
        return when (config.me.storage) {
            Storage.Redis -> RedisPersistOrdersStrategy(ordersDatabaseAccessorsHolder, stopOrdersDatabaseAccessorsHolder, config)
            Storage.RedisWithoutOrders -> AzurePersistOrdersStrategy(ordersDatabaseAccessorsHolder, stopOrdersDatabaseAccessorsHolder)
            else -> null
        }
    }
}
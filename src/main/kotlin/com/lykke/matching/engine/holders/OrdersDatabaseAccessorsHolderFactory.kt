package com.lykke.matching.engine.holders

import com.lykke.matching.engine.database.Storage
import com.lykke.matching.engine.database.file.FileOrderBookDatabaseAccessor
import com.lykke.matching.engine.database.redis.InitialLoadingRedisHolder
import com.lykke.matching.engine.database.redis.accessor.impl.RedisOrderBookDatabaseAccessor
import com.lykke.matching.engine.utils.config.Config
import org.springframework.beans.factory.FactoryBean
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import java.util.Optional

@Component
class OrdersDatabaseAccessorsHolderFactory : FactoryBean<OrdersDatabaseAccessorsHolder> {

    @Autowired
    private lateinit var config: Config

    @Autowired
    private lateinit var redisHolder: Optional<InitialLoadingRedisHolder>

    override fun getObjectType(): Class<*> {
        return OrdersDatabaseAccessorsHolder::class.java
    }

    override fun getObject(): OrdersDatabaseAccessorsHolder {
        return when (config.me.storage) {
            Storage.Azure ->
                OrdersDatabaseAccessorsHolder(FileOrderBookDatabaseAccessor(config.me.orderBookPath), null)
            Storage.Redis ->
                OrdersDatabaseAccessorsHolder(RedisOrderBookDatabaseAccessor(redisHolder.get(), config.me.redis.ordersDatabase),
                        if (config.me.writeOrdersToSecondaryDb)
                            FileOrderBookDatabaseAccessor(config.me.secondaryOrderBookPath)
                        else null)
        }
    }
}
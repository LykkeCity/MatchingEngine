package com.lykke.matching.engine.holders

import com.lykke.matching.engine.database.Storage
import com.lykke.matching.engine.database.file.FileStopOrderBookDatabaseAccessor
import com.lykke.matching.engine.database.redis.accessor.impl.RedisStopOrderBookDatabaseAccessor
import com.lykke.matching.engine.database.redis.connection.RedisConnection
import com.lykke.matching.engine.utils.config.Config
import org.springframework.beans.factory.FactoryBean
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import java.util.Optional

@Component
class StopOrdersDatabaseAccessorsHolderFactory : FactoryBean<StopOrdersDatabaseAccessorsHolder> {

    @Autowired
    private lateinit var config: Config

    @Autowired
    private lateinit var initialLoadingRedisConnection: Optional<RedisConnection>

    override fun getObjectType(): Class<*> {
        return StopOrdersDatabaseAccessorsHolder::class.java
    }

    override fun getObject(): StopOrdersDatabaseAccessorsHolder {
        return when (config.me.storage) {
            Storage.Azure ->
                StopOrdersDatabaseAccessorsHolder(FileStopOrderBookDatabaseAccessor(config.me.stopOrderBookPath), null)
            Storage.Redis ->
                StopOrdersDatabaseAccessorsHolder(RedisStopOrderBookDatabaseAccessor(initialLoadingRedisConnection.get(), config.me.redis.ordersDatabase),
                        if (config.me.writeOrdersToSecondaryDb)
                            FileStopOrderBookDatabaseAccessor(config.me.secondaryStopOrderBookPath)
                        else null)
            Storage.RedisWithoutOrders ->
                StopOrdersDatabaseAccessorsHolder(FileStopOrderBookDatabaseAccessor(config.me.stopOrderBookPath), null)
        }
    }
}
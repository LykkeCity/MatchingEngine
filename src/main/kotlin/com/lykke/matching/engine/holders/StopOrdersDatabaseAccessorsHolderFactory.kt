package com.lykke.matching.engine.holders

import com.lykke.matching.engine.database.Storage
import com.lykke.matching.engine.database.file.FileStopOrderBookDatabaseAccessor
import com.lykke.matching.engine.database.redis.accessor.impl.RedisStopOrderBookDatabaseAccessor
import com.lykke.matching.engine.utils.config.Config
import org.springframework.beans.factory.FactoryBean
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import redis.clients.jedis.JedisPool

@Component
class StopOrdersDatabaseAccessorsHolderFactory : FactoryBean<StopOrdersDatabaseAccessorsHolder> {

    @Autowired
    private lateinit var config: Config

    @Autowired
    private lateinit var jedisPool: JedisPool

    override fun getObjectType(): Class<*> {
        return StopOrdersDatabaseAccessorsHolder::class.java
    }

    override fun getObject(): StopOrdersDatabaseAccessorsHolder {
        return when (config.me.storage) {
            Storage.Azure ->
                StopOrdersDatabaseAccessorsHolder(FileStopOrderBookDatabaseAccessor(config.me.stopOrderBookPath), null)
            Storage.Redis ->
                StopOrdersDatabaseAccessorsHolder(RedisStopOrderBookDatabaseAccessor(jedisPool, config.me.redis.ordersDatabase),
                        if (config.me.writeOrdersToSecondaryDb)
                            FileStopOrderBookDatabaseAccessor(config.me.stopOrderBookPath)
                        else null)
        }
    }
}
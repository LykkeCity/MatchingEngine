package com.lykke.matching.engine.database.listeners

import com.lykke.matching.engine.common.QueueConsumer
import com.lykke.matching.engine.database.MidPriceDatabaseAccessor
import com.lykke.matching.engine.database.common.entity.MidPricePersistenceData
import com.lykke.matching.engine.database.reconciliation.events.MidPricesPersistEvent
import com.lykke.matching.engine.database.redis.connection.RedisConnection
import org.apache.log4j.Logger
import org.springframework.util.CollectionUtils
import redis.clients.jedis.Transaction
import java.util.concurrent.BlockingQueue
import javax.annotation.PostConstruct
import kotlin.concurrent.thread

class MidPricesPersistListener(private val redisConnection: RedisConnection,
                               private val redisMidPriceDatabaseAccessor: MidPriceDatabaseAccessor,
                               private val persistMidPricesQueue: BlockingQueue<MidPricesPersistEvent>) : QueueConsumer<MidPricesPersistEvent> {
    private companion object {
        val LOGGER = Logger.getLogger(MidPricesPersistListener::class.java)
    }

    @PostConstruct
    private fun init() {
        thread(name = "${MidPricesPersistListener::class.java.name}.midPricesAsyncWriter") {
            while (true) {
                val midPricePersistenceData = persistMidPricesQueue.take().midPricePersistenceData
                redisConnection.transactionalResource { transaction ->
                    persistMidPrices(transaction, midPricePersistenceData)
                    transaction.exec()
                }
            }
        }
    }

    private fun persistMidPrices(transaction: Transaction, midPricePersistenceData: MidPricePersistenceData) {
        if (midPricePersistenceData.removeAll) {
            LOGGER.info("Remove all mid prices")
            redisMidPriceDatabaseAccessor.removeAll(transaction)
            return
        }

        if (!CollectionUtils.isEmpty(midPricePersistenceData.midPrices)) {
            redisMidPriceDatabaseAccessor.save(transaction, midPricePersistenceData.midPrices!!)
        }
    }
}
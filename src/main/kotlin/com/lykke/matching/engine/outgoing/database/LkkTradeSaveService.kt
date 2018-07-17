package com.lykke.matching.engine.outgoing.database

import com.lykke.matching.engine.daos.LkkTrade
import com.lykke.matching.engine.database.MarketOrderDatabaseAccessor
import com.lykke.utils.logging.ThrottlingLogger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import java.util.concurrent.BlockingQueue
import javax.annotation.PostConstruct
import kotlin.concurrent.thread

@Service
class LkkTradeSaveService @Autowired constructor(private val marketOrderDatabaseAccessor: MarketOrderDatabaseAccessor) {

    companion object {
        private val LOGGER = ThrottlingLogger.getLogger(LkkTradeSaveService::class.java.name)
    }

    @Autowired
    private lateinit var lkkTradesQueue: BlockingQueue<List<LkkTrade>>

    @PostConstruct
    fun initialize() {
        thread(start = true, name = LkkTradeSaveService::class.java.name) {
            while (true) {
                process(lkkTradesQueue.take())
            }
        }
    }

    fun process(lkkTrades: List<LkkTrade>) {
        try {
            marketOrderDatabaseAccessor.addLkkTrades(lkkTrades)
        } catch (e: Exception) {
            LOGGER.error("Unable to save trade", e)
        }
    }
}
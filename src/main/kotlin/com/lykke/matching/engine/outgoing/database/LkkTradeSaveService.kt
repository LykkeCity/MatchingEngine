package com.lykke.matching.engine.outgoing.database

import com.lykke.matching.engine.daos.LkkTrade
import com.lykke.matching.engine.database.MarketOrderDatabaseAccessor
import com.lykke.utils.logging.ThrottlingLogger
import java.util.concurrent.BlockingQueue

class LkkTradeSaveService(private val marketOrderDatabaseAccessor: MarketOrderDatabaseAccessor,
                          private val trades: BlockingQueue<List<LkkTrade>>) : Thread() {

    companion object {
        private val LOGGER = ThrottlingLogger.getLogger(LkkTradeSaveService::class.java.name)
    }

    override fun run() {
        while (true) {
            try {
                marketOrderDatabaseAccessor.addLkkTrades(trades.take())
            } catch (e: Exception) {
                LOGGER.error("Unable to save trade", e)
            }
        }
    }

}
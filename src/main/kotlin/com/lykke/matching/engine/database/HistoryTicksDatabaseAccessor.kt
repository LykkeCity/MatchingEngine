package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.TickUpdateInterval
import com.lykke.matching.engine.history.TickBlobHolder

interface HistoryTicksDatabaseAccessor {
    fun loadHistoryTicks(): List<TickBlobHolder>
    fun loadHistoryTick(asset: String, tickUpdateInterval: TickUpdateInterval): TickBlobHolder?
    fun saveHistoryTick(tick: TickBlobHolder)
}
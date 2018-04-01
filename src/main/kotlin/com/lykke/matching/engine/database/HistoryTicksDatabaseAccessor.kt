package com.lykke.matching.engine.database

import com.lykke.matching.engine.history.TickBlobHolder

interface HistoryTicksDatabaseAccessor {
    fun loadHistoryTicks(): List<TickBlobHolder>
    fun saveHistoryTick(tick: TickBlobHolder)
}
package com.lykke.matching.engine.database

import com.lykke.matching.engine.history.TickBlobHolder

class TestHistoryTicksDatabaseAccessor : HistoryTicksDatabaseAccessor{
    private val ticks = ArrayList<TickBlobHolder>()

    override fun loadHistoryTicks(): List<TickBlobHolder> {
        return ticks
    }

    override fun saveHistoryTick(tick: TickBlobHolder) {
        ticks.add(tick)
    }

    fun clear() {
        ticks.clear()
    }
}
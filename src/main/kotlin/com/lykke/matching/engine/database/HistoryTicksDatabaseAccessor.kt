package com.lykke.matching.engine.database

import com.lykke.matching.engine.history.TickBlobHolder
import com.microsoft.azure.storage.blob.CloudBlob

interface HistoryTicksDatabaseAccessor {
    fun loadHistoryTick(asset: String, period: String) : CloudBlob?
    fun loadHistoryTicks() : List<CloudBlob>
    fun saveHistoryTick(tick: TickBlobHolder)
}
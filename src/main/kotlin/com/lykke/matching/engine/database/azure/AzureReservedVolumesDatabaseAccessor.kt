package com.lykke.matching.engine.database.azure

import com.lykke.matching.engine.daos.balance.ReservedVolumeCorrection
import com.lykke.matching.engine.daos.azure.balance.AzureReservedVolumeCorrection
import com.lykke.matching.engine.database.ReservedVolumesDatabaseAccessor
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import com.microsoft.azure.storage.table.CloudTable
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.util.Date

@Component
class AzureReservedVolumesDatabaseAccessor(connectionString: String) : ReservedVolumesDatabaseAccessor {

    companion object {
        val LOGGER = ThrottlingLogger.getLogger(AzureReservedVolumesDatabaseAccessor::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    @Value("\${azure.reserved.volumes.table}")
    private lateinit var tableName: String


    private val reservedVolumesTable: CloudTable = getOrCreateTable(connectionString, tableName)

    override fun addCorrectionsInfo(corrections: List<ReservedVolumeCorrection>) {
        try {
            val now = Date()
            batchInsertOrMerge(reservedVolumesTable, corrections.map { AzureReservedVolumeCorrection(now, it.clientId, it.assetId, it.orderIds, it.oldReserved, it.newReserved) })
        } catch (e: Exception) {
            val message = "Unable to save reserved volumes corrections, size: ${corrections.size}"
            LOGGER.error(message, e)
            METRICS_LOGGER.logError(message, e)
        }
    }
}
package com.lykke.matching.engine.database.azure

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.azure.AzureAsset
import com.lykke.matching.engine.database.BackOfficeDatabaseAccessor
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import com.microsoft.azure.storage.table.CloudTable
import com.microsoft.azure.storage.table.TableOperation
import com.microsoft.azure.storage.table.TableQuery
import java.util.HashMap

class AzureBackOfficeDatabaseAccessor constructor (connectionString: String) : BackOfficeDatabaseAccessor {

    companion object {
        private val LOGGER = ThrottlingLogger.getLogger(AzureBackOfficeDatabaseAccessor::class.java.name)
        private val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    val assetsTable: CloudTable = getOrCreateTable(connectionString, "Dictionaries")

    private val ASSET = "Asset"

    override fun loadAssets(): MutableMap<String, Asset> {
        val result = HashMap<String, Asset>()

        try {
            val partitionQuery = TableQuery.from(AzureAsset::class.java)
                    .where(TableQuery.generateFilterCondition("PartitionKey", TableQuery.QueryComparisons.EQUAL, ASSET))

            for (asset in assetsTable.execute(partitionQuery)) {
                result[asset.assetId] = Asset(asset.assetId, asset.accuracy)
            }
        } catch(e: Exception) {
            LOGGER.error("Unable to load assets", e)
            METRICS_LOGGER.logError("Unable to load assets", e)
        }

        return result
    }

    override fun loadAsset(assetId: String): Asset? {
        try {
            val retrieveAssetAsset = TableOperation.retrieve(ASSET, assetId, AzureAsset::class.java)
            val asset = assetsTable.execute(retrieveAssetAsset).getResultAsType<AzureAsset>()
            if (asset != null) {
               return Asset(asset.assetId, asset.accuracy)
            }
        } catch(e: Exception) {
            LOGGER.error("Unable to load assetId: $assetId", e)
            METRICS_LOGGER.logError("Unable to load assetId: $assetId", e)
        }
        return null
    }

}
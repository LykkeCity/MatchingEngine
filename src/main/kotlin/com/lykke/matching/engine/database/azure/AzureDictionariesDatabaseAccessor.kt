package com.lykke.matching.engine.database.azure

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.azure.AzureAssetPair
import com.lykke.matching.engine.database.DictionariesDatabaseAccessor
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import com.microsoft.azure.storage.table.CloudTable
import com.microsoft.azure.storage.table.TableOperation
import com.microsoft.azure.storage.table.TableQuery
import java.util.HashMap

class AzureDictionariesDatabaseAccessor(dictsConfig: String): DictionariesDatabaseAccessor {

    companion object {
        private val LOGGER = ThrottlingLogger.getLogger(AzureDictionariesDatabaseAccessor::class.java.name)
        private val METRICS_LOGGER = MetricsLogger.getLogger()
        private const val ASSET_PAIR = "AssetPair"
    }

    private val assetsTable: CloudTable = getOrCreateTable(dictsConfig, "Dictionaries")

    override fun loadAssetPairs(): HashMap<String, AssetPair> {
        val result = HashMap<String, AssetPair>()

        try {
            val partitionQuery = TableQuery.from(AzureAssetPair::class.java)
                    .where(TableQuery.generateFilterCondition("PartitionKey", TableQuery.QueryComparisons.EQUAL, ASSET_PAIR))

            for (asset in assetsTable.execute(partitionQuery)) {
                result[asset.assetPairId] = AssetPair(asset.assetPairId, asset.baseAssetId, asset.quotingAssetId, asset.accuracy, asset.minVolume, asset.minInvertedVolume)
            }
        } catch(e: Exception) {
            LOGGER.error("Unable to load asset pairs", e)
            METRICS_LOGGER.logError( "Unable to load asset pairs", e)
        }

        return result
    }

    override fun loadAssetPair(assetId: String): AssetPair? {
        try {
            val retrieveAssetPair = TableOperation.retrieve(ASSET_PAIR, assetId, AzureAssetPair::class.java)
            val assetPair = assetsTable.execute(retrieveAssetPair).getResultAsType<AzureAssetPair>()
            if (assetPair != null) {
                return AssetPair(assetPair.assetPairId, assetPair.baseAssetId, assetPair.quotingAssetId, assetPair.accuracy, assetPair.minVolume, assetPair.minInvertedVolume)
            }
        } catch(e: Exception) {
            LOGGER.error("Unable to load asset: $assetId", e)
            METRICS_LOGGER.logError( "Unable to load asset: $assetId", e)
        }
        return null
    }
}
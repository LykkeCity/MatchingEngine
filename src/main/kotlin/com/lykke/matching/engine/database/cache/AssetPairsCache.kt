package com.lykke.matching.engine.database.cache

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.database.DictionariesDatabaseAccessor
import com.lykke.matching.engine.services.events.NewAssetPairsEvent
import com.lykke.matching.engine.services.events.RemovedAssetPairsEvent
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.ApplicationEventPublisher
import org.springframework.stereotype.Component
import java.util.stream.Collectors
import kotlin.concurrent.fixedRateTimer

@Component
class AssetPairsCache @Autowired constructor(
        private val databaseAccessor: DictionariesDatabaseAccessor,
        private val applicationEventPublisher: ApplicationEventPublisher,
        @Value("\${application.assets.pair.cache.update.interval}") updateInterval: Long? = null) : DataCache() {

    private val knownAssetPairs = HashSet<String>()

    companion object {
        private val LOGGER = LoggerFactory.getLogger(AssetPairsCache::class.java)
    }

    @Volatile
    private var assetPairsById: Map<String, AssetPair> = HashMap()

    @Volatile
    private var assetPairsByPair: Map<String, AssetPair> = HashMap()

    init {
        this.assetPairsById = databaseAccessor.loadAssetPairs()
        this.assetPairsByPair = generateAssetPairsMapByPair(assetPairsById)
        assetPairsById.values.forEach { knownAssetPairs.add(it.assetPairId) }
        LOGGER.info("Loaded ${assetPairsById.size} assets pairs")
        updateInterval?.let {
            fixedRateTimer(name = "Asset Pairs Cache Updater", initialDelay = it, period = it) {
                update()
            }
        }
    }

    fun getAssetPair(assetPair: String): AssetPair? {
        return assetPairsById[assetPair] ?: databaseAccessor.loadAssetPair(assetPair)
    }

    fun getAssetPair(assetId1: String, assetId2: String): AssetPair? {
        return assetPairsByPair[pairKey(assetId1, assetId2)] ?: assetPairsByPair[pairKey(assetId2, assetId1)]
    }

    fun getAssetPairByAssetId(assetId: String): Set<AssetPair> {
        return assetPairsById
                .values
                .stream()
                .filter { it.quotingAssetId == assetId || it.baseAssetId == assetId }
                .collect(Collectors.toSet())
    }

    override fun update() {
        val newMap = databaseAccessor.loadAssetPairs()
        val oldAssetPairs = assetPairsById.values
        if (newMap.isNotEmpty()) {
            val newMapByPair = generateAssetPairsMapByPair(newMap)
            assetPairsById = newMap
            assetPairsByPair = newMapByPair
        }

        val newAssetPairs = getNewAssetPairs(this.assetPairsById.values)
        val removedAssetPairs = getRemovedAssetPairs(this.assetPairsById.values, oldAssetPairs)
        if (newAssetPairs.isNotEmpty()) {
            knownAssetPairs.addAll(newAssetPairs.map { it.assetPairId })
            applicationEventPublisher.publishEvent(NewAssetPairsEvent(newAssetPairs))
        }

        if (removedAssetPairs.isNotEmpty()) {
            knownAssetPairs.removeAll(removedAssetPairs.map { it.assetPairId })
            applicationEventPublisher.publishEvent(RemovedAssetPairsEvent(removedAssetPairs))
        }
    }

    private fun getNewAssetPairs(newAssetPairs: Collection<AssetPair>): Collection<AssetPair> {
        return newAssetPairs.filter { assetPair ->
            knownAssetPairs.contains(assetPair.assetPairId)
        }
    }

    private fun getRemovedAssetPairs(newAssetPairs: Collection<AssetPair>, prevAssetPair: Collection<AssetPair>): Collection<AssetPair> {
        val allNewAssetPairs = newAssetPairs.map { it.assetPairId }.toHashSet()
        return prevAssetPair.filter { assetPair ->
            !allNewAssetPairs.contains(assetPair.assetPairId)
        }
    }

    private fun generateAssetPairsMapByPair(assetPairsById: Map<String, AssetPair>): Map<String, AssetPair> {
        return assetPairsById.values
                .groupBy { pairKey(it.baseAssetId, it.quotingAssetId) }
                .mapValues {
                    if (it.value.size > 1) {
                        LOGGER.error("Asset pairs count for baseAssetId=${it.value.first().baseAssetId} and quotingAssetId=${it.value.first().quotingAssetId} is more than 1")
                    }
                    it.value.first()
                }
    }

    private fun pairKey(assetId1: String, assetId2: String) = "${assetId1}_$assetId2"
}
package com.lykke.matching.engine.holders

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.database.cache.AssetsCache
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component

@Component
class AssetsHolder @Autowired constructor (private val assetsCache: AssetsCache) {
    fun getAsset(assetId: String): Asset {
        return getAssetAllowNulls(assetId) ?: throw Exception("Unable to find asset $assetId")
    }

    fun getAssetAllowNulls(assetId: String): Asset? {
        return assetsCache.getAsset(assetId)
    }
}
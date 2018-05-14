package com.lykke.matching.engine.holders

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.database.cache.AssetsCache
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

@Service
class AssetsHolder @Autowired constructor (private val assetsCache: AssetsCache) {
    fun getAsset(assetId: String): Asset {
        val asset = assetsCache.getAsset(assetId)
        if (asset == null) {
            throw Exception("Unable to find asset $assetId")
        }
        return asset
    }
}
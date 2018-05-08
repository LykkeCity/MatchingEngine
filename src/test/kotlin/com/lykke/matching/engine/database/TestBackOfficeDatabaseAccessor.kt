package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.Asset
import org.springframework.stereotype.Component
import java.util.HashMap

@Component
class TestBackOfficeDatabaseAccessor: BackOfficeDatabaseAccessor {
    val assets = HashMap<String, Asset>()

    fun addAsset(asset: Asset) {
        assets[asset.assetId] = asset
    }

    override fun loadAsset(assetId: String): Asset? {
        return assets[assetId]
    }

    override fun loadAssets(): MutableMap<String, Asset> {
        return HashMap()
    }

    fun clear() {
        assets.clear()
    }
}
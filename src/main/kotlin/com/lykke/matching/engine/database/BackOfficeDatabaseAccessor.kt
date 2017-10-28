package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.Asset

interface BackOfficeDatabaseAccessor {
    fun loadAssets(): MutableMap<String, Asset>
    fun loadAsset(assetId: String): Asset?
}
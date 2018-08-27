package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.Settings
import java.math.BigDecimal

class TestConfigDatabaseAccessor : SettingsDatabaseAccessor {
    private var settings = Settings(emptySet(), emptySet(), emptyMap(), emptyMap())

    override fun getAllEnabledSettingGroups(): Settings {
        return settings
    }

    fun addTrustedClient(trustedClient: String) {
        val trustedClients: MutableSet<String> = HashSet(settings.trustedClients)
        trustedClients.add(trustedClient)

        settings = Settings(trustedClients = trustedClients,
                disabledAssets = settings.disabledAssets,
                moPriceDeviationThresholds = settings.moPriceDeviationThresholds,
                loPriceDeviationThresholds = settings.loPriceDeviationThresholds)
    }

    fun addDisabledAsset(disabledAsset: String) {
        val disabledAssets: MutableSet<String> = HashSet(settings.disabledAssets)
        disabledAssets.add(disabledAsset)

        settings = Settings(trustedClients = settings.trustedClients,
                disabledAssets = disabledAssets,
                moPriceDeviationThresholds = settings.moPriceDeviationThresholds,
                loPriceDeviationThresholds = settings.loPriceDeviationThresholds)
    }

    fun addMarketOrderPriceDeviationThreshold(assetPirId: String, threshold: BigDecimal) {
        val thresholds = HashMap(settings.moPriceDeviationThresholds)
        thresholds[assetPirId] = threshold
        settings = Settings(settings.trustedClients, settings.disabledAssets, thresholds, settings.loPriceDeviationThresholds)
    }

    fun addLimitOrderPriceDeviationThreshold(assetPirId: String, threshold: BigDecimal) {
        val thresholds = HashMap(settings.loPriceDeviationThresholds)
        thresholds[assetPirId] = threshold
        settings = Settings(settings.trustedClients, settings.disabledAssets, settings.moPriceDeviationThresholds, thresholds)
    }

    fun clear() {
        settings = Settings(emptySet(), emptySet(), emptyMap(), emptyMap())
    }
}
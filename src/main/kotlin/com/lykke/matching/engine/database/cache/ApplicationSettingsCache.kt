package com.lykke.matching.engine.database.cache

import com.lykke.matching.engine.daos.setting.SettingsGroup
import com.lykke.matching.engine.database.SettingsDatabaseAccessor
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.math.BigDecimal
import kotlin.concurrent.fixedRateTimer

@Component
class ApplicationSettingsCache @Autowired constructor (private val settingsDatabaseAccessor: SettingsDatabaseAccessor,
                                                       @Value("\${application.settings.update.interval}") updateInterval: Long) : DataCache() {

    private companion object {
        private const val DISABLED_ASSETS = "DisabledAssets"
        private const val TRUSTED_CLIENTS = "TrustedClients"
        private const val MO_PRICE_DEVIATION_THRESHOLD = "MarketOrderPriceDeviationThreshold"
        private const val LO_PRICE_DEVIATION_THRESHOLD = "LimitOrderPriceDeviationThreshold"
    }

    private lateinit var trustedClients: Set<String>
    private lateinit var disabledAssets: Set<String>
    private lateinit var moPriceDeviationThresholds: Map<String, BigDecimal>,
    private lateinit var loPriceDeviationThresholds: Map<String, BigDecimal>

    init {
        update()
        fixedRateTimer(name = "Application Properties Updater", initialDelay = updateInterval, period = updateInterval, daemon = true) {
            update()
        }
    }

    @Synchronized
    override fun update() {
        val allSettingGroups = settingsDatabaseAccessor.getAllSettingGroups(true)

        trustedClients = getSettingNameToValueByGroupName(allSettingGroups, TRUSTED_CLIENTS).values.toSet()
        disabledAssets = getSettingNameToValueByGroupName(allSettingGroups, DISABLED_ASSETS).values.toSet()
        moPriceDeviationThresholds = getSettingNameToValueByGroupName(allSettingGroups, MO_PRICE_DEVIATION_THRESHOLD).mapValues { BigDecimal(it.value)}
        loPriceDeviationThresholds = getSettingNameToValueByGroupName(allSettingGroups, LO_PRICE_DEVIATION_THRESHOLD).mapValues { BigDecimal(it.value)}
    }

    @Synchronized
    fun isTrustedClient(client: String): Boolean {
        return trustedClients.contains(client)
    }

    @Synchronized
    fun isAssetDisabled(asset: String): Boolean {
        return disabledAssets.contains(asset)
    }

    @Synchronized
    fun marketOrderPriceDeviationThreshold(assetPairId: String): BigDecimal? {
        return moPriceDeviationThresholds[assetPairId]
    }

    @Synchronized
    fun limitOrderPriceDeviationThreshold(assetPairId: String): BigDecimal? {
        return loPriceDeviationThresholds[assetPairId]
    }

    @Synchronized
    fun addTrustedClient(clientName: String, clientId: String) {

    }

    @Synchronized
    fun removeTrustedClient(clientName: String) {

    }

    @Synchronized
    fun addDisabledAsset(assetName: String, assetId: String) {

    }

    @Synchronized
    fun removeDisabledAsset() {

    }

    private fun getSettingNameToValueByGroupName(settingGroupName: Set<SettingsGroup>, name: String): Map<String, String> {
        val settings = settingGroupName.find { it.name == name } ?: return emptyMap()

        val result = HashMap<String, String>()
        settings.settings.forEach { result.put(it.name, it.value) }

        return result
    }
}
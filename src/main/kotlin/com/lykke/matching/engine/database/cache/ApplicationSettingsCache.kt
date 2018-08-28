package com.lykke.matching.engine.database.cache

import com.lykke.matching.engine.daos.setting.SettingsGroup
import com.lykke.matching.engine.database.SettingsDatabaseAccessor
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import kotlin.collections.HashSet
import kotlin.concurrent.fixedRateTimer

@Component
class ApplicationSettingsCache @Autowired constructor(private val settingsDatabaseAccessor: SettingsDatabaseAccessor,
                                                      @Value("\${application.settings.update.interval}") updateInterval: Long) : DataCache() {

    private companion object {
        private const val DISABLED_ASSETS = "DisabledAssets"
        private const val TRUSTED_CLIENTS = "TrustedClients"
        private const val MO_PRICE_DEVIATION_THRESHOLD = "MarketOrderPriceDeviationThreshold"
        private const val LO_PRICE_DEVIATION_THRESHOLD = "LimitOrderPriceDeviationThreshold"
    }

    @Volatile
    private lateinit var trustedClients: MutableSet<String>

    @Volatile
    private lateinit var disabledAssets: MutableSet<String>

    @Volatile
    private lateinit var moPriceDeviationThresholds: MutableMap<String, BigDecimal>

    @Volatile
    private lateinit var loPriceDeviationThresholds: MutableMap<String, BigDecimal>

    init {
        update()
        fixedRateTimer(name = "Application Properties Updater", initialDelay = updateInterval, period = updateInterval, daemon = true) {
            update()
        }
    }

    override fun update() {
        val allSettingGroups = settingsDatabaseAccessor.getAllSettingGroups(true)
        trustedClients = getSettingValues(allSettingGroups, TRUSTED_CLIENTS)
        disabledAssets = getSettingValues(allSettingGroups, DISABLED_ASSETS)

        moPriceDeviationThresholds = ConcurrentHashMap(getSettingNameToValueByGroupName(allSettingGroups, MO_PRICE_DEVIATION_THRESHOLD)
                .mapValues { BigDecimal(it.value) })
        loPriceDeviationThresholds = ConcurrentHashMap(getSettingNameToValueByGroupName(allSettingGroups, LO_PRICE_DEVIATION_THRESHOLD)
                .mapValues { BigDecimal(it.value) })
    }

    fun isTrustedClient(client: String): Boolean {
        return trustedClients.contains(client)
    }

    fun isAssetDisabled(asset: String): Boolean {
        return disabledAssets.contains(asset)
    }

    fun marketOrderPriceDeviationThreshold(assetPairId: String): BigDecimal? {
        return moPriceDeviationThresholds[assetPairId]
    }

    fun limitOrderPriceDeviationThreshold(assetPairId: String): BigDecimal? {
        return loPriceDeviationThresholds[assetPairId]
    }

    fun addOrUpdateMarketOrderPriceDeviationThreshold(assetPairId: String, value: BigDecimal) {
        moPriceDeviationThresholds.put(assetPairId, value)
    }

    fun deleteMarketOrderPriceDeviationThreshold(assetPairId: String) {
        moPriceDeviationThresholds.remove(assetPairId)
    }

    fun deleteAllMarketOrderPriceDeviationThresholds() {
        moPriceDeviationThresholds.clear()
    }

    fun addOrUpdateLimitOrderPriceDeviationThreshold(assetPairId: String, value: BigDecimal) {
        loPriceDeviationThresholds.put(assetPairId, value)
    }

    fun deleteLimitOrdersLimitOrderPriceDeviationThreshold(assetPairId: String) {
        loPriceDeviationThresholds.remove(assetPairId)
    }

    fun deleteAllLimitOrdersLimitOrderPriceDeviationThresholds() {
        loPriceDeviationThresholds.clear()
    }

    fun addTrustedClient(clientId: String) {
        trustedClients.add(clientId)
    }

    fun deleteTrustedClient(clientId: String) {
        trustedClients.remove(clientId)
    }

    fun deleteAllTrustedClients() {
        trustedClients.clear()
    }

    fun addDisabledAsset(assetId: String) {
        disabledAssets.add(assetId)
    }

    fun deleteDisabledAsset(assetId: String) {
        disabledAssets.remove(assetId)
    }

    fun deleteAllDisabledAssets() {
        disabledAssets.clear()
    }

    private fun getSettingNameToValueByGroupName(settingGroupName: Set<SettingsGroup>, name: String): Map<String, String> {
        val settings = settingGroupName.find { it.name == name } ?: return emptyMap()

        val result = HashMap<String, String>()
        settings.settings.forEach { result.put(it.name, it.value) }

        return result
    }

    private fun getSettingValues(settingGroupName: Set<SettingsGroup>, name: String): MutableSet<String> {
        return Collections.synchronizedSet(HashSet(getSettingNameToValueByGroupName(settingGroupName, name).values))
    }
}
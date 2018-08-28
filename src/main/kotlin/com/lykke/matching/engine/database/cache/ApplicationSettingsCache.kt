package com.lykke.matching.engine.database.cache

import com.lykke.matching.engine.daos.setting.AvailableSettingGroups
import com.lykke.matching.engine.daos.setting.SettingsGroup
import com.lykke.matching.engine.database.SettingsDatabaseAccessor
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.util.concurrent.ConcurrentHashMap
import kotlin.concurrent.fixedRateTimer

@Component
class ApplicationSettingsCache @Autowired constructor(private val settingsDatabaseAccessor: SettingsDatabaseAccessor,
                                                      @Value("\${application.settings.update.interval}") updateInterval: Long) : DataCache() {

    @Volatile
    private lateinit var trustedClients: MutableMap<String, String>

    @Volatile
    private lateinit var disabledAssets: MutableMap<String, String>

    @Volatile
    private lateinit var moPriceDeviationThresholds: MutableMap<String, String>

    @Volatile
    private lateinit var loPriceDeviationThresholds: MutableMap<String, String>

    init {
        update()
        fixedRateTimer(name = "Application Properties Updater", initialDelay = updateInterval, period = updateInterval, daemon = true) {
            update()
        }
    }

    override fun update() {
        val allSettingGroups = settingsDatabaseAccessor.getAllSettingGroups(true)
        trustedClients = getSettingNameToValueByGroupName(allSettingGroups, AvailableSettingGroups.TRUSTED_CLIENTS)
        disabledAssets = getSettingNameToValueByGroupName(allSettingGroups, AvailableSettingGroups.DISABLED_ASSETS)

        moPriceDeviationThresholds = getSettingNameToValueByGroupName(allSettingGroups, AvailableSettingGroups.MO_PRICE_DEVIATION_THRESHOLD)
        loPriceDeviationThresholds = getSettingNameToValueByGroupName(allSettingGroups, AvailableSettingGroups.LO_PRICE_DEVIATION_THRESHOLD)
    }

    fun isTrustedClient(client: String): Boolean {
        return trustedClients.values.contains(client)
    }

    fun isAssetDisabled(asset: String): Boolean {
        return disabledAssets.values.contains(asset)
    }

    fun marketOrderPriceDeviationThreshold(assetPairId: String): BigDecimal? {
        return moPriceDeviationThresholds[assetPairId]?.toBigDecimal()
    }

    fun limitOrderPriceDeviationThreshold(assetPairId: String): BigDecimal? {
        return loPriceDeviationThresholds[assetPairId]?.toBigDecimal()
    }

    fun createOrUpdateSettingValue(settingGroup: AvailableSettingGroups, settingName: String, value: String) {
        getSettingNameToBalueByGroup(settingGroup)[settingName] = value
    }

    fun deleteSetting(settingGroup: AvailableSettingGroups, settingName: String) {
        getSettingNameToBalueByGroup(settingGroup).remove(settingName)
    }

    fun deleteSettingGroup(settingGroup: AvailableSettingGroups) {
        getSettingNameToBalueByGroup(settingGroup).clear()
    }

    private fun getSettingNameToValueByGroupName(settingGroupName: Set<SettingsGroup>, availableSettingGroups: AvailableSettingGroups): MutableMap<String, String> {
        val settings = settingGroupName.find { it.name == availableSettingGroups.name } ?: return ConcurrentHashMap()

        val result = ConcurrentHashMap<String, String>()
        settings.settings.forEach { result.put(it.name, it.value) }

        return result
    }

    private fun getSettingNameToBalueByGroup(settingGroup: AvailableSettingGroups):  MutableMap<String, String> {
        return when (settingGroup) {
            AvailableSettingGroups.TRUSTED_CLIENTS -> trustedClients
            AvailableSettingGroups.DISABLED_ASSETS -> disabledAssets
            AvailableSettingGroups.MO_PRICE_DEVIATION_THRESHOLD -> moPriceDeviationThresholds
            AvailableSettingGroups.LO_PRICE_DEVIATION_THRESHOLD -> loPriceDeviationThresholds
        }
    }
}
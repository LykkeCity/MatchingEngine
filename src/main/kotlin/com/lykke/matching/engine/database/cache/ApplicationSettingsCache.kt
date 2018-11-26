package com.lykke.matching.engine.database.cache

import com.lykke.matching.engine.daos.setting.AvailableSettingGroup
import com.lykke.matching.engine.daos.setting.SettingsGroup
import com.lykke.matching.engine.database.SettingsDatabaseAccessor
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.util.concurrent.ConcurrentHashMap
import javax.annotation.PostConstruct

@Component
class ApplicationSettingsCache @Autowired constructor(private val settingsDatabaseAccessor: SettingsDatabaseAccessor) : DataCache() {
    @Volatile
    private var trustedClients: MutableMap<String, String> = ConcurrentHashMap()

    @Volatile
    private var disabledAssets: MutableMap<String, String> = ConcurrentHashMap()

    @Volatile
    private var moPriceDeviationThresholds: MutableMap<String, String> = ConcurrentHashMap()

    @Volatile
    private var loPriceDeviationThresholds: MutableMap<String, String> = ConcurrentHashMap()

    @Volatile
    private var messageProcessingSwitch: MutableMap<String, String> = ConcurrentHashMap()

    @PostConstruct
    override fun update() {
        val allSettingGroups = settingsDatabaseAccessor.getAllSettingGroups(true)
        trustedClients = getSettingNameToValueByGroupName(allSettingGroups, AvailableSettingGroup.TRUSTED_CLIENTS)
        disabledAssets = getSettingNameToValueByGroupName(allSettingGroups, AvailableSettingGroup.DISABLED_ASSETS)
        messageProcessingSwitch = getSettingNameToValueByGroupName(allSettingGroups, AvailableSettingGroup.MESSAGE_PROCESSING_SWITCH)

        moPriceDeviationThresholds = getSettingNameToValueByGroupName(allSettingGroups, AvailableSettingGroup.MO_PRICE_DEVIATION_THRESHOLD)
        loPriceDeviationThresholds = getSettingNameToValueByGroupName(allSettingGroups, AvailableSettingGroup.LO_PRICE_DEVIATION_THRESHOLD)
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

    fun isMessageProcessingEnabled(): Boolean {
        return messageProcessingSwitch.isEmpty()
    }

    fun createOrUpdateSettingValue(settingGroup: AvailableSettingGroup, settingName: String, value: String) {
        getSettingNameToValueByGroup(settingGroup)[settingName] = value
    }

    fun deleteSetting(settingGroup: AvailableSettingGroup, settingName: String) {
        getSettingNameToValueByGroup(settingGroup).remove(settingName)
    }

    fun deleteSettingGroup(settingGroup: AvailableSettingGroup) {
        getSettingNameToValueByGroup(settingGroup).clear()
    }

    private fun getSettingNameToValueByGroupName(settingGroups: Set<SettingsGroup>, availableSettingGroups: AvailableSettingGroup): MutableMap<String, String> {
        val settings = settingGroups.find { it.settingGroup == availableSettingGroups } ?: return ConcurrentHashMap()

        val result = ConcurrentHashMap<String, String>()
        settings.settings.forEach { result.put(it.name, it.value) }

        return result
    }

    private fun getSettingNameToValueByGroup(settingGroup: AvailableSettingGroup): MutableMap<String, String> {
        return when (settingGroup) {
            AvailableSettingGroup.TRUSTED_CLIENTS -> trustedClients
            AvailableSettingGroup.DISABLED_ASSETS -> disabledAssets
            AvailableSettingGroup.MO_PRICE_DEVIATION_THRESHOLD -> moPriceDeviationThresholds
            AvailableSettingGroup.LO_PRICE_DEVIATION_THRESHOLD -> loPriceDeviationThresholds
            AvailableSettingGroup.MESSAGE_PROCESSING_SWITCH -> messageProcessingSwitch
        }
    }
}
package com.lykke.matching.engine.database.cache

import com.lykke.matching.engine.daos.setting.AvailableSettingGroup
import com.lykke.matching.engine.daos.setting.Setting
import com.lykke.matching.engine.daos.setting.SettingsGroup
import com.lykke.matching.engine.database.SettingsDatabaseAccessor
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import org.springframework.util.CollectionUtils
import java.math.BigDecimal
import javax.annotation.PostConstruct

@Component
class ApplicationSettingsCache @Autowired constructor(private val settingsDatabaseAccessor: SettingsDatabaseAccessor) : DataCache() {
    private val settingsByGroupName = HashMap<String, MutableSet<Setting>>()

    @PostConstruct
    @Synchronized
    override fun update() {
        val settingGroups = settingsDatabaseAccessor.getAllSettingGroups()

        AvailableSettingGroup.values().forEach { settingGroup ->
            val dbSettings = settingGroups.find { it.name == settingGroup.settingGroupName }?.let {
                HashSet(it.settings)
            }

            if (CollectionUtils.isEmpty(dbSettings)) {
                settingsByGroupName.remove(settingGroup.settingGroupName)
            } else {
                settingsByGroupName[settingGroup.settingGroupName] = dbSettings!!
            }
        }
    }

    @Synchronized
    fun isTrustedClient(client: String): Boolean {
        return getSettingsForSettingGroup(AvailableSettingGroup.TRUSTED_CLIENTS)?.find { it.enabled && it.value == client } != null
    }

    @Synchronized
    fun isAssetDisabled(asset: String): Boolean {
        return getSettingsForSettingGroup(AvailableSettingGroup.DISABLED_ASSETS)?.find { it.enabled && it.value == asset } != null
    }

    @Synchronized
    fun marketOrderPriceDeviationThreshold(assetPairId: String): BigDecimal? {
        return getSettingsForSettingGroup(AvailableSettingGroup.MO_PRICE_DEVIATION_THRESHOLD)
                ?.find { it.enabled && it.name == assetPairId }
                ?.value
                ?.toBigDecimal()
    }

    @Synchronized
    fun limitOrderPriceDeviationThreshold(assetPairId: String): BigDecimal? {
        return getSettingsForSettingGroup(AvailableSettingGroup.LO_PRICE_DEVIATION_THRESHOLD)
                ?.find { it.enabled && it.name == assetPairId }
                ?.value
                ?.toBigDecimal()
    }

    @Synchronized
    fun createOrUpdateSettingValue(settingGroup: AvailableSettingGroup, settingName: String, value: String, enabled: Boolean) {
        deleteSetting(settingGroup, settingName)
        val settings = settingsByGroupName.getOrPut(settingGroup.settingGroupName) { HashSet() }
        settings.add(Setting(settingName, value, enabled))
    }

    @Synchronized
    fun deleteSetting(settingGroup: AvailableSettingGroup, settingName: String) {
        val settings = getSettingsForSettingGroup(settingGroup)
        settings?.removeIf { it.name == settingName }
        if (CollectionUtils.isEmpty(settings)) {
            settingsByGroupName.remove(settingGroup.settingGroupName)
        }
    }

    @Synchronized
    fun deleteSettingGroup(settingGroup: AvailableSettingGroup) {
        settingsByGroupName.remove(settingGroup.settingGroupName)
    }

    @Synchronized
    fun getAllSettingGroups(enabled: Boolean?): Set<SettingsGroup> {
        return settingsByGroupName
                .map { entry -> SettingsGroup(entry.key, entry.value.filter { enabled == null || it.enabled == enabled }.toSet()) }
                .toSet()
    }

    @Synchronized
    fun getSettingsGroup(settingGroupName: String, enabled: Boolean? = null): SettingsGroup? {
        return settingsByGroupName[settingGroupName]?.filter { enabled == null || it.enabled == enabled }?.let {
            SettingsGroup(settingGroupName, it.toSet())
        }
    }

    @Synchronized
    fun getSetting(settingGroupName: String, settingName: String, enabled: Boolean? = null): Setting? {
        return getSettingsGroup(settingGroupName, enabled)?.let {
            it.settings.find { it.name == settingName }
        }
    }

    private fun getSettingsForSettingGroup(settingGroup: AvailableSettingGroup): MutableSet<Setting>? {
        return settingsByGroupName[settingGroup.settingGroupName]
    }
}
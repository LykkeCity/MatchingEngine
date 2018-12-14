package com.lykke.matching.engine.holders

import com.lykke.matching.engine.daos.setting.AvailableSettingGroup
import com.lykke.matching.engine.daos.setting.Setting
import com.lykke.matching.engine.daos.setting.SettingsGroup
import com.lykke.matching.engine.database.cache.ApplicationGroupDeleteEvent
import com.lykke.matching.engine.database.cache.ApplicationSettingDeleteEvent
import com.lykke.matching.engine.database.cache.ApplicationSettingCreateOrUpdateEvent
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.util.concurrent.ConcurrentHashMap
import javax.annotation.PostConstruct

@Component
class ApplicationSettingsHolder(val applicationSettingsCache: ApplicationSettingsCache) {

    @Volatile
    private var trustedClients: MutableMap<String, String> = ConcurrentHashMap()

    @Volatile
    private var disabledAssets: MutableMap<String, String> = ConcurrentHashMap()

    @Volatile
    private var moPriceDeviationThresholds: MutableMap<String, String> = ConcurrentHashMap()

    @Volatile
    private var midPriceDeviationThresholds: MutableMap<String, String> = ConcurrentHashMap()

    @Volatile
    private var messageProcessingSwitch: MutableMap<String, String> = ConcurrentHashMap()

    @PostConstruct
    fun update() {
        val allSettingGroups = applicationSettingsCache.getAllSettingGroups(true)

        trustedClients = getSettingValueByName(allSettingGroups, AvailableSettingGroup.TRUSTED_CLIENTS)
        disabledAssets = getSettingValueByName(allSettingGroups, AvailableSettingGroup.DISABLED_ASSETS)
        messageProcessingSwitch = getSettingValueByName(allSettingGroups, AvailableSettingGroup.MESSAGE_PROCESSING_SWITCH)

        moPriceDeviationThresholds = getSettingValueByName(allSettingGroups, AvailableSettingGroup.MO_PRICE_DEVIATION_THRESHOLD)
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

    fun midPriceDeviationThreshold(assetPairId: String): BigDecimal? {
        return midPriceDeviationThresholds[assetPairId]?.toBigDecimal()
    }

    fun isMessageProcessingEnabled(): Boolean {
        return messageProcessingSwitch.isEmpty()
    }

    @EventListener
    private fun onSettingCreate(event: ApplicationSettingCreateOrUpdateEvent) {
        val settingValueByName = getSettingNameToValueByGroup(event.settingGroup) ?: return
        settingValueByName[event.setting.name] = event.setting.value
    }

    @EventListener
    private fun onSettingRemove(event: ApplicationSettingDeleteEvent) {
        val settingValueByName = getSettingNameToValueByGroup(event.settingGroup) ?: return
        settingValueByName.remove(event.setting.name)
    }

    @EventListener
    private fun onSettingGroupRemove(event: ApplicationGroupDeleteEvent) {
        getSettingNameToValueByGroup(event.availableSettingGroup)?.clear()
    }

    private fun getSettingValueByName(settings: Set<Setting>): MutableMap<String, String> {
        val result = ConcurrentHashMap<String, String>()
        settings.forEach { result[it.name] = it.value }

        return result
    }

    private fun getSettingValueByName(settingGroups: Set<SettingsGroup>, availableSettingGroup: AvailableSettingGroup): MutableMap<String, String> {
        val group = settingGroups.find { it.settingGroup == availableSettingGroup } ?: return ConcurrentHashMap()

        return getSettingValueByName(group.settings)
    }

    private fun getSettingNameToValueByGroup(settingGroup: AvailableSettingGroup): MutableMap<String, String>? {
        return when (settingGroup) {
            AvailableSettingGroup.TRUSTED_CLIENTS -> trustedClients
            AvailableSettingGroup.DISABLED_ASSETS -> disabledAssets
            AvailableSettingGroup.MO_PRICE_DEVIATION_THRESHOLD -> moPriceDeviationThresholds
            AvailableSettingGroup.MESSAGE_PROCESSING_SWITCH -> messageProcessingSwitch
            else -> null
        }
    }
}
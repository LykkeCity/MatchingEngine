package com.lykke.matching.engine.holders

import com.lykke.matching.engine.daos.DisableFunctionalityRule
import com.lykke.matching.engine.daos.setting.AvailableSettingGroup
import com.lykke.matching.engine.database.cache.ApplicationGroupDeleteEvent
import com.lykke.matching.engine.database.cache.ApplicationSettingDeleteEvent
import com.lykke.matching.engine.database.cache.ApplicationSettingUpdateEvent
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import org.nustaq.serialization.FSTConfiguration
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component
import java.util.concurrent.ConcurrentHashMap
import javax.annotation.PostConstruct

@Component
class DisabledFunctionalityRulesHolder(val applicationSettingsCache: ApplicationSettingsCache) {
    private val disabledFunctionalityRules = ConcurrentHashMap.newKeySet<DisableFunctionalityRule>()
    private val conf = FSTConfiguration.createJsonConfiguration()

    @Autowired
    private lateinit var assetsPairsHolder: AssetsPairsHolder

    fun isDisabled(rule: DisableFunctionalityRule): Boolean {
        if (rule.isEmpty() || disabledFunctionalityRules.isEmpty()) {
            return false
        }

        return disabledFunctionalityRules.count {
            isRuleMatch(rule, it)
        } > 0
    }

    @PostConstruct
    private fun init() {
        applicationSettingsCache.getSettingsGroup(AvailableSettingGroup.DISABLED_FUNCTIONALITY_RULES, true)?.settings?.forEach {
            disabledFunctionalityRules.add(conf.asObject(it.value.toByteArray()) as DisableFunctionalityRule)
        }
    }

    @EventListener
    private fun onSettincCreate(applicationSettingUpdateEvent: ApplicationSettingUpdateEvent) {
        val setting = applicationSettingUpdateEvent.setting
        if (applicationSettingUpdateEvent.settingGroup != AvailableSettingGroup.DISABLED_FUNCTIONALITY_RULES) {
            return
        }

        disabledFunctionalityRules.add(conf.asObject(setting.value.toByteArray()) as DisableFunctionalityRule)
    }

    @EventListener
    private fun onSettingRemove(applicationSettingDeleteEvent: ApplicationSettingDeleteEvent) {
        if (applicationSettingDeleteEvent.settingGroup != AvailableSettingGroup.DISABLED_FUNCTIONALITY_RULES) {
            return
        }

        disabledFunctionalityRules.remove(conf.asObject(applicationSettingDeleteEvent.setting.value.toByteArray()) as DisableFunctionalityRule)
    }

    @EventListener
    private fun onSettingGroupRemove(applicationGroupDeleteEvent: ApplicationGroupDeleteEvent) {
        if (applicationGroupDeleteEvent.availableSettingGroup != AvailableSettingGroup.DISABLED_FUNCTIONALITY_RULES) {
            return
        }

        disabledFunctionalityRules.clear()
    }

    private fun isRuleMatch(inputRequest: DisableFunctionalityRule, disableRule: DisableFunctionalityRule): Boolean {
        var assetsPair = inputRequest.assetPairId?.let {
            assetsPairsHolder.getAssetPair(inputRequest.assetPairId)
        }

        return (disableRule.assetId == null ||
                inputRequest.assetId == disableRule.assetId
                || assetsPair?.baseAssetId == disableRule.assetId
                || assetsPair?.quotingAssetId == disableRule.assetId)
                && (disableRule.assetPairId == null || inputRequest.assetPairId == disableRule.assetPairId)
                && (disableRule.messageType == null || inputRequest.messageType == disableRule.messageType)
    }
}
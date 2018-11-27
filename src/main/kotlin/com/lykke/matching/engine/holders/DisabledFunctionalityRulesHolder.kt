package com.lykke.matching.engine.holders

import com.google.gson.Gson
import com.lykke.matching.engine.daos.DisabledFunctionalityRule
import com.lykke.matching.engine.daos.setting.AvailableSettingGroup
import com.lykke.matching.engine.database.cache.ApplicationGroupDeleteEvent
import com.lykke.matching.engine.database.cache.ApplicationSettingDeleteEvent
import com.lykke.matching.engine.database.cache.ApplicationSettingUpdateEvent
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component
import org.springframework.util.StringUtils
import java.util.concurrent.ConcurrentHashMap
import javax.annotation.PostConstruct

@Component
class DisabledFunctionalityRulesHolder(val applicationSettingsCache: ApplicationSettingsCache) {
    private val disabledFunctionalityRules = ConcurrentHashMap.newKeySet<DisabledFunctionalityRule>()

    @Autowired
    private lateinit var gson: Gson

    @Autowired
    private lateinit var assetsPairsHolder: AssetsPairsHolder

    fun isDisabled(rule: DisabledFunctionalityRule): Boolean {
        if (rule.isEmpty() || disabledFunctionalityRules.isEmpty()) {
            return false
        }

        return disabledFunctionalityRules.any {
            isRuleMatch(rule, it)
        }
    }

    @PostConstruct
    private fun init() {
        applicationSettingsCache.getSettingsGroup(AvailableSettingGroup.DISABLED_FUNCTIONALITY_RULES, true)?.settings?.forEach {
            disabledFunctionalityRules.add(gson.fromJson(it.value, DisabledFunctionalityRule::class.java))
        }
    }

    @EventListener
    private fun onSettincCreate(applicationSettingUpdateEvent: ApplicationSettingUpdateEvent) {
        val setting = applicationSettingUpdateEvent.setting
        if (applicationSettingUpdateEvent.settingGroup != AvailableSettingGroup.DISABLED_FUNCTIONALITY_RULES) {
            return
        }

        disabledFunctionalityRules.add(gson.fromJson(setting.value, DisabledFunctionalityRule::class.java))
    }

    @EventListener
    private fun onSettingRemove(applicationSettingDeleteEvent: ApplicationSettingDeleteEvent) {
        if (applicationSettingDeleteEvent.settingGroup != AvailableSettingGroup.DISABLED_FUNCTIONALITY_RULES) {
            return
        }

        disabledFunctionalityRules.remove(gson.fromJson(applicationSettingDeleteEvent.setting.value, DisabledFunctionalityRule::class.java))
    }

    @EventListener
    private fun onSettingGroupRemove(applicationGroupDeleteEvent: ApplicationGroupDeleteEvent) {
        if (applicationGroupDeleteEvent.availableSettingGroup != AvailableSettingGroup.DISABLED_FUNCTIONALITY_RULES) {
            return
        }

        disabledFunctionalityRules.clear()
    }

    private fun isRuleMatch(inputRequest: DisabledFunctionalityRule, disableRule: DisabledFunctionalityRule): Boolean {
        return (StringUtils.isEmpty(disableRule.assetId) || StringUtils.isEmpty(inputRequest.assetId) || inputRequest.assetId == disableRule.assetId)
                && isAssetPairMatch(inputRequest.assetPairId, disableRule)
                && (disableRule.messageType == null || inputRequest.messageType == null || inputRequest.messageType == disableRule.messageType)
    }

    private fun isAssetPairMatch(assetPairId: String?, disableRule: DisabledFunctionalityRule): Boolean {
        if (StringUtils.isEmpty(assetPairId) || StringUtils.isEmpty(disableRule.assetId)) {
            return true
        }

        val assetPair = assetsPairsHolder.getAssetPair(assetPairId!!)

        return assetPair.baseAssetId == disableRule.assetId
                || assetPair.quotingAssetId == disableRule.assetId
                || assetPairId == disableRule.assetPairId
    }

}
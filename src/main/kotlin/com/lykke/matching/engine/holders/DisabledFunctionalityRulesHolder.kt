package com.lykke.matching.engine.holders

import com.google.gson.Gson
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.DisabledFunctionalityRule
import com.lykke.matching.engine.daos.OperationType
import com.lykke.matching.engine.daos.setting.AvailableSettingGroup
import com.lykke.matching.engine.database.cache.ApplicationGroupDeleteEvent
import com.lykke.matching.engine.database.cache.ApplicationSettingCreateOrUpdateEvent
import com.lykke.matching.engine.database.cache.ApplicationSettingDeleteEvent
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component
import org.springframework.util.CollectionUtils
import org.springframework.util.StringUtils
import javax.annotation.PostConstruct

@Component
class DisabledFunctionalityRulesHolder(val applicationSettingsCache: ApplicationSettingsCache) {
    private val disabledFunctionalityRulesByAssetPairId = HashMap<String, MutableSet<DisabledFunctionalityRule>>()
    private val disabledFunctionalityRulesByAssetId = HashMap<String, MutableSet<DisabledFunctionalityRule>>()
    private val disabledFunctionalityRules = HashSet<DisabledFunctionalityRule>()

    @Autowired
    private lateinit var gson: Gson

    fun isDisabled(asset: Asset?, operationType: OperationType): Boolean {
        if (asset == null) {
            return disabledFunctionalityRules.any { isRuleMatch(null, null, operationType, it) }
        }

        val rules = disabledFunctionalityRulesByAssetId[asset.assetId] ?: return false

        return rules.any { isRuleMatch(null, asset, operationType, it) }
    }

    fun isDisabled(assetPair: AssetPair?, operationType: OperationType): Boolean {
        if (assetPair == null) {
            return disabledFunctionalityRules.any { isRuleMatch(null, null, operationType, it) }
        }

        val rules = getAllRulesForAssetPair(assetPair)

        if (CollectionUtils.isEmpty(rules)) {
            return false
        }

        return rules.any { isRuleMatch(assetPair, null, operationType, it) }
    }

    @PostConstruct
    private fun init() {
        applicationSettingsCache.getSettingsGroup(AvailableSettingGroup.DISABLED_FUNCTIONALITY_RULES, true)?.settings?.forEach {
            addRule(gson.fromJson(it.value, DisabledFunctionalityRule::class.java))
        }
    }

    @EventListener
    private fun onSettincUpdate(applicationSettingUpdateEvent: ApplicationSettingCreateOrUpdateEvent) {
        val setting = applicationSettingUpdateEvent.setting
        if (applicationSettingUpdateEvent.settingGroup != AvailableSettingGroup.DISABLED_FUNCTIONALITY_RULES) {
            return
        }

        val rule = gson.fromJson(setting.value, DisabledFunctionalityRule::class.java)
        if (setting.enabled) {
            addRule(rule)
        } else {
            removeRule(rule)
        }
    }

    @EventListener
    private fun onSettingRemove(applicationSettingDeleteEvent: ApplicationSettingDeleteEvent) {
        if (applicationSettingDeleteEvent.settingGroup != AvailableSettingGroup.DISABLED_FUNCTIONALITY_RULES) {
            return
        }

        removeRule(gson.fromJson(applicationSettingDeleteEvent.setting.value, DisabledFunctionalityRule::class.java))
    }

    @EventListener
    private fun onSettingGroupRemove(applicationGroupDeleteEvent: ApplicationGroupDeleteEvent) {
        if (applicationGroupDeleteEvent.availableSettingGroup != AvailableSettingGroup.DISABLED_FUNCTIONALITY_RULES) {
            return
        }

        clear()
    }

    private fun getAllRulesForAssetPair(assetPair: AssetPair): Set<DisabledFunctionalityRule> {
        val result = HashSet<DisabledFunctionalityRule>()
        disabledFunctionalityRulesByAssetPairId[assetPair.assetPairId]?.let {
            result.addAll(it)
        }

        disabledFunctionalityRulesByAssetId[assetPair.baseAssetId]?.let {
            result.addAll(it)
        }

        disabledFunctionalityRulesByAssetId[assetPair.quotingAssetId]?.let {
            result.addAll(it)
        }

        return result
    }

    private fun addRule(rule: DisabledFunctionalityRule) {
        if (rule.assetPairId == null && rule.assetId == null) {
            disabledFunctionalityRules.add(rule)
            return
        }

        if (rule.assetId != null) {
            val rules = disabledFunctionalityRulesByAssetId.getOrPut(rule.assetId) { HashSet() }
            rules.add(rule)
        }

        if (rule.assetPairId != null) {
            val rules = disabledFunctionalityRulesByAssetPairId.getOrPut(rule.assetPairId) { HashSet() }
            rules.add(rule)
        }

    }

    private fun clear() {
        disabledFunctionalityRules.clear()
        disabledFunctionalityRulesByAssetId.clear()
        disabledFunctionalityRulesByAssetPairId.clear()
    }

    private fun removeRule(rule: DisabledFunctionalityRule) {
        disabledFunctionalityRules.remove(rule)
        disabledFunctionalityRulesByAssetPairId[rule.assetPairId]?.remove(rule)
        disabledFunctionalityRulesByAssetId[rule.assetId]?.remove(rule)
    }

    private fun isRuleMatch(assetPair: AssetPair?,
                            asset: Asset?,
                            operationType: OperationType?,
                            disableRule: DisabledFunctionalityRule): Boolean {
        return (StringUtils.isEmpty(disableRule.assetId) || asset == null || asset.assetId == disableRule.assetId)
                && isAssetPairMatch(assetPair, disableRule)
                && (disableRule.operationType == null || operationType == null || operationType == disableRule.operationType)
    }

    private fun isAssetPairMatch(assetPair: AssetPair?, disableRule: DisabledFunctionalityRule): Boolean {
        if (assetPair == null || StringUtils.isEmpty(disableRule.assetId)) {
            return true
        }

        return assetPair.baseAssetId == disableRule.assetId
                || assetPair.quotingAssetId == disableRule.assetId
                || assetPair.assetPairId == disableRule.assetPairId
    }
}
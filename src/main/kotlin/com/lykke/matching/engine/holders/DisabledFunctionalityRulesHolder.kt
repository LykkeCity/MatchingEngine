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

    @Autowired
    private lateinit var assetsPairsHolder: AssetsPairsHolder

    fun isDisabled(asset: Asset?, operation: OperationType): Boolean {
        if (asset == null) {
            return disabledFunctionalityRules.any { isRuleMatch(DisabledFunctionalityRule(null, null, operation), it) }
        }

        val rules = disabledFunctionalityRulesByAssetId[asset.assetId] ?: return false

        return rules.any { isRuleMatch(DisabledFunctionalityRule(asset.assetId, null, operation), it) }
    }

    fun isDisabled(assetPair: AssetPair?, operation: OperationType): Boolean {
        if (assetPair == null) {
            return disabledFunctionalityRules.any { isRuleMatch(DisabledFunctionalityRule(null, null, operation), it) }
        }

        val rules = getAllRulesForAssetPair(assetPair)

        if (CollectionUtils.isEmpty(rules)) {
            return false
        }

        return rules.any { isRuleMatch(DisabledFunctionalityRule(null, assetPair.assetPairId, operation), it) }
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

    private fun isRuleMatch(inputRequest: DisabledFunctionalityRule, disableRule: DisabledFunctionalityRule): Boolean {
        return (StringUtils.isEmpty(disableRule.assetId) || StringUtils.isEmpty(inputRequest.assetId) || inputRequest.assetId == disableRule.assetId)
                && isAssetPairMatch(inputRequest.assetPairId, disableRule)
                && (disableRule.operationType == null || inputRequest.operationType == null || inputRequest.operationType == disableRule.operationType)
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
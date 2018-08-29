package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.setting.Setting
import com.lykke.matching.engine.daos.setting.SettingsGroup
import java.math.BigDecimal

class TestConfigDatabaseAccessor : SettingsDatabaseAccessor {


    private val trustedClients = HashMap<String, String>()
    private val disabledAssets = HashMap<String, String>()
    private val moPriceDeviationThresholds = HashMap<String, String>()
    private val loPriceDeviationThresholds = HashMap<String, String>()

    override fun getSetting(settingGroupName: String, settingName: String, enabled: Boolean?): Setting? {
        throw NotImplementedError()
    }

    override fun getSettingsGroup(settingGroupName: String, enabled: Boolean?): SettingsGroup? {
        throw NotImplementedError()
    }

    override fun getAllSettingGroups(enabled: Boolean?): Set<SettingsGroup> {
        throw NotImplementedError()
    }

    override fun createOrUpdateSetting(settingGroupName: String, setting: Setting) {
        throw NotImplementedError()
    }

    override fun deleteSetting(settingGroupName: String, settingName: String) {
        throw NotImplementedError()
    }

    override fun deleteSettingsGroup(settingGroupName: String) {
        throw NotImplementedError()
    }

    fun addTrustedClient(trustedClient: String) {
        trustedClients.put(trustedClient, trustedClient)
    }

    fun addDisabledAsset(disabledAsset: String) {
        disabledAssets.put(disabledAsset, disabledAsset)
    }

    fun addMarketOrderPriceDeviationThreshold(assetPirId: String, threshold: BigDecimal) {
        moPriceDeviationThresholds.put(assetPirId, threshold.toPlainString())
    }

    fun addLimitOrderPriceDeviationThreshold(assetPirId: String, threshold: BigDecimal) {
        loPriceDeviationThresholds.put(assetPirId, threshold.toPlainString())
    }

    fun clear() {
        trustedClients.clear()
        disabledAssets.clear()
        moPriceDeviationThresholds.clear()
        loPriceDeviationThresholds.clear()
    }
}
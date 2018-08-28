package com.lykke.matching.engine.daos.setting

enum class AvailableSettingGroups(val settingGroupName: String) {
    DISABLED_ASSETS("DisabledAssets"),
    TRUSTED_CLIENTS("TrustedClients"),
    MO_PRICE_DEVIATION_THRESHOLD("MarketOrderPriceDeviationThreshold"),
    LO_PRICE_DEVIATION_THRESHOLD("LimitOrderPriceDeviationThreshold");

    companion object {
        fun getBySettingsGroupName(settingGroupName: String): AvailableSettingGroups {
            return AvailableSettingGroups.values().find { it.settingGroupName == settingGroupName}
                    ?: throw IllegalArgumentException("Setting group with name $settingGroupName was not found")
        }
    }
}
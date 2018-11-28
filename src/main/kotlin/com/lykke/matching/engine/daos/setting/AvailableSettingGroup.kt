package com.lykke.matching.engine.daos.setting

enum class AvailableSettingGroup(val settingGroupName: String) {
    DISABLED_ASSETS("DisabledAssets"),
    TRUSTED_CLIENTS("TrustedClients"),
    MESSAGE_PROCESSING_SWITCH("MessageProcessingSwitch"),
    MO_PRICE_DEVIATION_THRESHOLD("MarketOrderPriceDeviationThreshold"),
    MID_PRICE_DEVIATION_THRESHOLD("MidPriceDeviationThreshold"),
    DISABLED_FUNCTIONALITY_RULES("DisabledFunctionalityRules");

    companion object {
        fun getBySettingsGroupName(settingGroupName: String): AvailableSettingGroup {
            return AvailableSettingGroup.values().find { it.settingGroupName == settingGroupName}
                    ?: throw InvalidSettingGroupException("Setting group with name $settingGroupName is not supported")
        }
    }
}
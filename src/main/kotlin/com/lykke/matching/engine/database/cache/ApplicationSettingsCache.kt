package com.lykke.matching.engine.database.cache

import com.lykke.matching.engine.daos.setting.AvailableSettingGroup
import com.lykke.matching.engine.daos.setting.Setting
import com.lykke.matching.engine.daos.setting.SettingsGroup
import com.lykke.matching.engine.database.SettingsDatabaseAccessor
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.ApplicationEventPublisher
import org.springframework.stereotype.Component
import org.springframework.util.CollectionUtils
import javax.annotation.PostConstruct

@Component
class ApplicationSettingsCache @Autowired constructor(private val settingsDatabaseAccessor: SettingsDatabaseAccessor,
                                                      private val applicationEventPublisher: ApplicationEventPublisher) : DataCache() {
    private val settingsByGroup = HashMap<AvailableSettingGroup, MutableSet<Setting>>()

    @PostConstruct
    @Synchronized
    override fun update() {
        val settingGroups = settingsDatabaseAccessor.getAllSettingGroups()

        AvailableSettingGroup.values().forEach { settingGroup ->
            val dbSettings = settingGroups.find { it.settingGroup == settingGroup }?.let {
                HashSet(it.settings)
            }

            if (CollectionUtils.isEmpty(dbSettings)) {
                settingsByGroup.remove(settingGroup)
            } else {
                settingsByGroup[settingGroup] = dbSettings!!
            }
        }
    }

    @Synchronized
    fun createOrUpdateSettingValue(settingGroup: AvailableSettingGroup, settingName: String, value: String, enabled: Boolean) {
        deleteSetting(settingGroup, settingName)
        val settings = settingsByGroup.getOrPut(settingGroup) { HashSet() }
        val settingToAdd = Setting(settingName, value, enabled)
        settings.add(settingToAdd)
        applicationEventPublisher.publishEvent(ApplicationSettingCreateOrUpdateEvent(settingGroup, settingToAdd))
    }

    @Synchronized
    fun deleteSetting(settingGroup: AvailableSettingGroup, settingName: String) {
        val settings = getSettingsForSettingGroup(settingGroup)
        val settingToRemove = settings?.find { it.name == settingName }
        settingToRemove?.let {
            settings.remove(settingToRemove)
            applicationEventPublisher.publishEvent(ApplicationSettingDeleteEvent(settingGroup, settingToRemove))
        }

        if (CollectionUtils.isEmpty(settings)) {
            settingsByGroup.remove(settingGroup)
        }
    }

    @Synchronized
    fun deleteSettingGroup(settingGroup: AvailableSettingGroup) {
        settingsByGroup.remove(settingGroup)
        applicationEventPublisher.publishEvent(ApplicationGroupDeleteEvent(settingGroup))
    }

    @Synchronized
    fun getAllSettingGroups(enabled: Boolean?): Set<SettingsGroup> {
        return settingsByGroup
                .map { entry -> SettingsGroup(entry.key, entry.value.filter { enabled == null || it.enabled == enabled }.toSet()) }
                .toSet()
    }

    @Synchronized
    fun getSettingsGroup(settingGroup: AvailableSettingGroup, enabled: Boolean? = null): SettingsGroup? {
        return settingsByGroup[settingGroup]?.filter { enabled == null || it.enabled == enabled }?.let {
            SettingsGroup(settingGroup, it.toSet())
        }
    }

    @Synchronized
    fun getSetting(settingGroup: AvailableSettingGroup, settingName: String, enabled: Boolean? = null): Setting? {
        return getSettingsGroup(settingGroup, enabled)?.let {
            it.settings.find { it.name == settingName }
        }
    }

    private fun getSettingsForSettingGroup(settingGroup: AvailableSettingGroup): MutableSet<Setting>? {
        return settingsByGroup[settingGroup]
    }
}
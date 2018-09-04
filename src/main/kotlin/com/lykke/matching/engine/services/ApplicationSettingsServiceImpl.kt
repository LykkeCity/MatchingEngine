package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.setting.AvailableSettingGroup
import com.lykke.matching.engine.daos.setting.Setting
import com.lykke.matching.engine.daos.setting.SettingHistoryRecord
import com.lykke.matching.engine.daos.setting.SettingsGroup
import com.lykke.matching.engine.database.SettingsDatabaseAccessor
import com.lykke.matching.engine.database.SettingsHistoryDatabaseAccessor
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.web.dto.SettingDto
import com.lykke.matching.engine.web.dto.SettingsGroupDto
import org.springframework.stereotype.Service

@Service
class ApplicationSettingsServiceImpl(private val settingsDatabaseAccessor: SettingsDatabaseAccessor,
                                     private val applicationSettingsCache: ApplicationSettingsCache,
                                     private val applicationSettingsHistoryDatabaseAccessor: SettingsHistoryDatabaseAccessor) : ApplicationSettingsService {

    private companion object {
        val COMMENT_FORMAT = "[%s] %s"
    }

    private enum class SettingOperation {
        ADD, UPDATE, ENABLE, DISABLE
    }

    override fun getAllSettingGroups(enabled: Boolean?): Set<SettingsGroupDto> {
        return settingsDatabaseAccessor.getAllSettingGroups(enabled).map { toSettingGroupDto(it) }.toSet()
    }

    override fun getSettingsGroup(settingsGroup: AvailableSettingGroup, enabled: Boolean?): SettingsGroupDto? {
        return settingsDatabaseAccessor.getSettingsGroup(settingsGroup.settingGroupName, enabled)?.let {
            toSettingGroupDto(it)
        }
    }

    override fun getSetting(settingsGroup: AvailableSettingGroup, settingName: String, enabled: Boolean?): SettingDto? {
        return settingsDatabaseAccessor.getSetting(settingsGroup.settingGroupName, settingName, enabled)?.let {
            toSettingDto(it)
        }
    }

    override fun getHistoryRecords(settingsGroup: AvailableSettingGroup, settingName: String): List<SettingDto> {
        return applicationSettingsHistoryDatabaseAccessor
                .get(settingsGroup.settingGroupName, settingName)
                .map(::toSettingDto)
    }

    override fun createOrUpdateSetting(settingsGroup: AvailableSettingGroup, settingDto: SettingDto) {
        val commentWithOperationPrefix = getCommentWithOperationPrefix(settingsGroup, settingDto)
        val settingToPersist = SettingDto(settingDto, commentWithOperationPrefix)

        settingsDatabaseAccessor.createOrUpdateSetting(settingsGroup.settingGroupName, toSetting(settingToPersist))
        addHistoryRecord(settingsGroup, settingToPersist)
        updateSettingInCache(settingToPersist, settingsGroup)
    }

    private fun updateSettingInCache(settingDto: SettingDto, settingsGroup: AvailableSettingGroup) {
        if (!settingDto.enabled!!) {
            applicationSettingsCache.deleteSetting(settingsGroup, settingDto.name)
        } else {
            applicationSettingsCache.createOrUpdateSettingValue(settingsGroup, settingDto.name, settingDto.value)
        }
    }

    private fun addHistoryRecord(settingGroupName: AvailableSettingGroup, settingDto: SettingDto) {
        applicationSettingsHistoryDatabaseAccessor.save(settingGroupName.settingGroupName, toSettingHistoryRecord(settingDto))
    }

    override fun deleteSettingsGroup(settingsGroup: AvailableSettingGroup) {
        settingsDatabaseAccessor.deleteSettingsGroup(settingsGroup.settingGroupName)
        applicationSettingsCache.deleteSettingGroup(settingsGroup)
    }

    override fun deleteSetting(settingsGroup: AvailableSettingGroup, settingName: String) {
        settingsDatabaseAccessor.deleteSetting(settingsGroup.settingGroupName, settingName)
        applicationSettingsCache.deleteSetting(settingsGroup, settingName)
    }

    private fun toSettingGroupDto(settingGroup: SettingsGroup): SettingsGroupDto {
        val settingsDtos = settingGroup.settings.map { toSettingDto(it) }.toSet()
        return SettingsGroupDto(settingGroup.name, settingsDtos)
    }

    private fun toSettingDto(setting: Setting): SettingDto {
        return SettingDto(setting.name, setting.value, setting.enabled)
    }

    private fun toSettingDto(settingHistoryRecord: SettingHistoryRecord): SettingDto {
        return settingHistoryRecord.let {
            SettingDto(it.name, it.value, it.enabled, it.comment, it.user, it.timestamp)
        }
    }

    private fun toSetting(settingDto: SettingDto): Setting {
        return Setting(settingDto.name, settingDto.value, settingDto.enabled!!)
    }

    private fun toSettingHistoryRecord(settingDto: SettingDto): SettingHistoryRecord {
        return settingDto.let {
            SettingHistoryRecord(it.name, it.value, it.enabled!!, it.comment!!, it.user!!)
        }
    }

    private fun getCommentWithOperationPrefix(settingsGroup: AvailableSettingGroup, setting: SettingDto): String {
        val previousSetting = getSetting(settingsGroup, setting.name)
        return COMMENT_FORMAT.format(getSettingOperation(previousSetting, setting), setting.comment)
    }

    private fun getSettingOperation(previousSettingState: SettingDto?, nextSettingState: SettingDto): SettingOperation {
        if (previousSettingState == null) {
            return SettingOperation.ADD
        }

        if (previousSettingState.enabled != nextSettingState.enabled) {
            return if (nextSettingState.enabled!!) SettingOperation.ENABLE else SettingOperation.DISABLE
        }

        return SettingOperation.UPDATE
    }
}
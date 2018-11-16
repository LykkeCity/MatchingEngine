package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.setting.*
import com.lykke.matching.engine.database.SettingsDatabaseAccessor
import com.lykke.matching.engine.database.SettingsHistoryDatabaseAccessor
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.web.dto.DeleteSettingRequestDto
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
        CREATE, UPDATE, ENABLE, DISABLE, DELETE
    }

    @Synchronized
    override fun getAllSettingGroups(enabled: Boolean?): Set<SettingsGroupDto> {
        return applicationSettingsCache.getAllSettingGroups(enabled).map { toSettingGroupDto(it) }.toSet()
    }

    @Synchronized
    override fun getSettingsGroup(settingsGroup: AvailableSettingGroup, enabled: Boolean?): SettingsGroupDto? {
        return applicationSettingsCache.getSettingsGroup(settingsGroup.settingGroupName, enabled)?.let {
            toSettingGroupDto(it)
        }
    }

    @Synchronized
    override fun getSetting(settingsGroup: AvailableSettingGroup, settingName: String, enabled: Boolean?): SettingDto? {
        return applicationSettingsCache.getSetting(settingsGroup.settingGroupName, settingName, enabled)?.let {
            toSettingDto(it)
        }
    }

    @Synchronized
    override fun getHistoryRecords(settingsGroup: AvailableSettingGroup, settingName: String): List<SettingDto> {
        return applicationSettingsHistoryDatabaseAccessor
                .get(settingsGroup.settingGroupName, settingName)
                .map(::toSettingDto)
    }

    @Synchronized
    override fun createOrUpdateSetting(settingsGroup: AvailableSettingGroup, settingDto: SettingDto) {
        val commentWithOperationPrefix = getCommentWithOperationPrefix(settingsGroup, settingDto)

        val setting = toSetting(settingDto)
        settingsDatabaseAccessor.createOrUpdateSetting(settingsGroup.settingGroupName, setting)
        addHistoryRecord(settingsGroup, commentWithOperationPrefix, settingDto.user!!, setting)
        updateSettingInCache(settingDto, settingsGroup)
    }

    @Synchronized
    override fun deleteSettingsGroup(settingsGroup: AvailableSettingGroup, deleteSettingRequestDto: DeleteSettingRequestDto) {
        val settingsGroupToBeDeleted = applicationSettingsCache.getSettingsGroup(settingsGroup.settingGroupName)
                ?: return

        settingsDatabaseAccessor.deleteSettingsGroup(settingsGroup.settingGroupName)
        val commentWithPrefix = getCommentWithOperationPrefix(SettingOperation.DELETE, deleteSettingRequestDto.comment)
        settingsGroupToBeDeleted.settings.forEach { addHistoryRecord(settingsGroup, commentWithPrefix, deleteSettingRequestDto.user, it) }
        applicationSettingsCache.deleteSettingGroup(settingsGroup)
    }

    @Synchronized
    override fun deleteSetting(settingsGroup: AvailableSettingGroup, settingName: String, deleteSettingRequestDto: DeleteSettingRequestDto) {
        val settingToBeDeleted = applicationSettingsCache.getSetting(settingsGroup.settingGroupName, settingName)
                ?: throw SettingNotFoundException("Setting with name '$settingName' not found")

        settingsDatabaseAccessor.deleteSetting(settingsGroup.settingGroupName, settingName)
        addHistoryRecord(settingsGroup,
                getCommentWithOperationPrefix(SettingOperation.DELETE, deleteSettingRequestDto.comment),
                deleteSettingRequestDto.user, settingToBeDeleted)
        applicationSettingsCache.deleteSetting(settingsGroup, settingName)
    }

    @Synchronized
    private fun updateSettingInCache(settingDto: SettingDto, settingsGroup: AvailableSettingGroup) {
        applicationSettingsCache.createOrUpdateSettingValue(settingsGroup, settingDto.name, settingDto.value, settingDto.enabled!!)
    }

    private fun addHistoryRecord(settingGroupName: AvailableSettingGroup, comment: String, user: String, setting: Setting) {
        applicationSettingsHistoryDatabaseAccessor.save(settingGroupName.settingGroupName, toSettingHistoryRecord(setting, comment, user))
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

    private fun toSettingHistoryRecord(settingDto: Setting, comment: String, user: String): SettingHistoryRecord {
        return settingDto.let {
            SettingHistoryRecord(it.name, it.value, it.enabled, comment, user)
        }
    }

    private fun getCommentWithOperationPrefix(prefix: SettingOperation, comment: String): String {
        return COMMENT_FORMAT.format(prefix, comment)
    }

    private fun getCommentWithOperationPrefix(settingsGroup: AvailableSettingGroup, setting: SettingDto): String {
        val previousSetting = getSetting(settingsGroup, setting.name)
        return getCommentWithOperationPrefix(getSettingOperation(previousSetting, setting), setting.comment!!)
    }

    private fun getSettingOperation(previousSettingState: SettingDto?, nextSettingState: SettingDto): SettingOperation {
        if (previousSettingState == null) {
            return SettingOperation.CREATE
        }

        if (previousSettingState.enabled != nextSettingState.enabled) {
            return if (nextSettingState.enabled!!) SettingOperation.ENABLE else SettingOperation.DISABLE
        }

        return SettingOperation.UPDATE
    }
}
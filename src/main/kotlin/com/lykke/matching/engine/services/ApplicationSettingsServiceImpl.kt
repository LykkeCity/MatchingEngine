package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.setting.*
import com.lykke.matching.engine.database.SettingsDatabaseAccessor
import com.lykke.matching.engine.database.SettingsHistoryDatabaseAccessor
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.services.events.DeleteSettingEvent
import com.lykke.matching.engine.services.events.DeleteSettingGroupEvent
import com.lykke.matching.engine.services.events.SettingChangedEvent
import com.lykke.matching.engine.services.validators.settings.SettingValidator
import com.lykke.matching.engine.web.dto.DeleteSettingRequestDto
import com.lykke.matching.engine.web.dto.SettingDto
import com.lykke.matching.engine.web.dto.SettingsGroupDto
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.ApplicationEventPublisher
import org.springframework.stereotype.Service

@Service
class ApplicationSettingsServiceImpl(private val settingsDatabaseAccessor: SettingsDatabaseAccessor,
                                     private val applicationSettingsCache: ApplicationSettingsCache,
                                     private val applicationSettingsHistoryDatabaseAccessor: SettingsHistoryDatabaseAccessor,
                                     private val applicationEventPublisher: ApplicationEventPublisher) : ApplicationSettingsService {

    private companion object {
        val COMMENT_FORMAT = "[%s] %s"
    }

    private enum class SettingOperation {
        CREATE, UPDATE, ENABLE, DISABLE, DELETE
    }

    @Autowired
    private lateinit var settingGroupToValidator: Map<AvailableSettingGroup, List<SettingValidator>>

    @Synchronized
    override fun getAllSettingGroups(enabled: Boolean?): Set<SettingsGroupDto> {
        return applicationSettingsCache.getAllSettingGroups(enabled).map { toSettingGroupDto(it) }.toSet()
    }

    @Synchronized
    override fun getSettingsGroup(settingsGroup: AvailableSettingGroup, enabled: Boolean?): SettingsGroupDto? {
        return applicationSettingsCache.getSettingsGroup(settingsGroup, enabled)?.let {
            toSettingGroupDto(it)
        }
    }

    @Synchronized
    override fun getSetting(settingsGroup: AvailableSettingGroup, settingName: String, enabled: Boolean?): SettingDto? {
        return applicationSettingsCache.getSetting(settingsGroup, settingName, enabled)?.let {
            toSettingDto(it)
        }
    }

    @Synchronized
    override fun getHistoryRecords(settingsGroupName: String, settingName: String): List<SettingDto> {
        return applicationSettingsHistoryDatabaseAccessor
                .get(settingsGroupName, settingName)
                .map(::toSettingDto)
    }

    @Synchronized
    override fun createOrUpdateSetting(settingsGroup: AvailableSettingGroup, settingDto: SettingDto) {
        settingGroupToValidator[settingsGroup]?.forEach { it.validate(settingDto) }
        val previousSetting = getSetting(settingsGroup, settingDto.name)

        val commentWithOperationPrefix = getCommentWithOperationPrefix(settingsGroup, settingDto)

        val setting = toSetting(settingDto)
        settingsDatabaseAccessor.createOrUpdateSetting(settingsGroup, setting)
        addHistoryRecord(settingsGroup, commentWithOperationPrefix, settingDto.user!!, setting)
        updateSettingInCache(settingDto, settingsGroup)

        applicationEventPublisher.publishEvent(SettingChangedEvent(settingsGroup,
                setting,
                previousSetting?.let{toSetting(it)},
                settingDto.comment!!,
                settingDto.user))
    }

    @Synchronized
    override fun deleteSettingsGroup(settingsGroup: AvailableSettingGroup, deleteSettingRequestDto: DeleteSettingRequestDto) {
        val settingsGroupToBeDeleted = applicationSettingsCache.getSettingsGroup(settingsGroup)
                ?: return

        settingsDatabaseAccessor.deleteSettingsGroup(settingsGroup)
        val commentWithPrefix = getCommentWithOperationPrefix(SettingOperation.DELETE, deleteSettingRequestDto.comment)
        settingsGroupToBeDeleted.settings.forEach { addHistoryRecord(settingsGroup, commentWithPrefix, deleteSettingRequestDto.user, it) }
        applicationSettingsCache.deleteSettingGroup(settingsGroup)

        applicationEventPublisher.publishEvent(DeleteSettingGroupEvent(settingsGroup,
                settingsGroupToBeDeleted.settings,
                deleteSettingRequestDto.comment,
                deleteSettingRequestDto.user))
    }

    @Synchronized
    override fun deleteSetting(settingsGroup: AvailableSettingGroup, settingName: String, deleteSettingRequestDto: DeleteSettingRequestDto) {
        val settingToBeDeleted = applicationSettingsCache.getSetting(settingsGroup, settingName)
                ?: throw SettingNotFoundException("Setting with name '$settingName' not found")

        settingsDatabaseAccessor.deleteSetting(settingsGroup, settingName)
        addHistoryRecord(settingsGroup,
                getCommentWithOperationPrefix(SettingOperation.DELETE, deleteSettingRequestDto.comment),
                deleteSettingRequestDto.user, settingToBeDeleted)
        applicationSettingsCache.deleteSetting(settingsGroup, settingName)


        applicationEventPublisher.publishEvent(DeleteSettingEvent(settingsGroup,
                settingToBeDeleted,
                deleteSettingRequestDto.comment,
                deleteSettingRequestDto.user))
    }

    @Synchronized
    private fun updateSettingInCache(settingDto: SettingDto, settingsGroup: AvailableSettingGroup) {
        applicationSettingsCache.createOrUpdateSettingValue(settingsGroup, settingDto.name, settingDto.value, settingDto.enabled!!)
    }

    private fun addHistoryRecord(settingGroup: AvailableSettingGroup, comment: String, user: String, setting: Setting) {
        applicationSettingsHistoryDatabaseAccessor.save(toSettingHistoryRecord(settingGroup.settingGroupName, setting, comment, user))
    }

    private fun toSettingGroupDto(settingGroup: SettingsGroup): SettingsGroupDto {
        val settingsDtos = settingGroup.settings.map { toSettingDto(it) }.toSet()
        return SettingsGroupDto(settingGroup.settingGroup.settingGroupName, settingsDtos)
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

    private fun toSettingHistoryRecord(settingsGroupName: String,
                                       setting: Setting,
                                       comment: String,
                                       user: String): SettingHistoryRecord {
        return setting.let {
            SettingHistoryRecord(settingsGroupName, it.name, it.value, it.enabled, comment, user)
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
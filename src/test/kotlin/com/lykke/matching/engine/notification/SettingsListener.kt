package com.lykke.matching.engine.notification

import com.lykke.matching.engine.daos.setting.AvailableSettingGroup
import com.lykke.matching.engine.services.events.ApplicationSettingDeletedEvent
import com.lykke.matching.engine.services.events.ApplicationGroupDeletedEvent
import com.lykke.matching.engine.services.events.ApplicationSettingCreatedOrUpdatedEvent
import org.springframework.context.event.EventListener
import java.util.concurrent.ConcurrentHashMap

class SettingsListener() {
    private val settingGroupToSettingChangeEvent = ConcurrentHashMap<AvailableSettingGroup, MutableList<ApplicationSettingCreatedOrUpdatedEvent>>()
    private val settingGroupToDeleteEvent = ConcurrentHashMap<AvailableSettingGroup, MutableList<ApplicationSettingDeletedEvent>>()
    private val settingGroupToDeleteGroupEvent = ConcurrentHashMap<AvailableSettingGroup, MutableList<ApplicationGroupDeletedEvent>>()

    @EventListener
    fun settingChanged(settingChangedEvent: ApplicationSettingCreatedOrUpdatedEvent) {
        val events = settingGroupToSettingChangeEvent.getOrPut(settingChangedEvent.settingGroup) {ArrayList()}
        events!!.add(settingChangedEvent)
    }

    @EventListener
    private fun settingDeleted(deleteSettingEvent: ApplicationSettingDeletedEvent) {
        val events = settingGroupToDeleteEvent.getOrPut(deleteSettingEvent.settingGroup) {ArrayList()}
        events!!.add(deleteSettingEvent)
    }

    @EventListener
    private fun settingGroupDeleted(deleteSettingGroupEvent: ApplicationGroupDeletedEvent) {
        val events = settingGroupToDeleteGroupEvent.getOrPut(deleteSettingGroupEvent.settingGroup) {ArrayList()}
        events!!.add(deleteSettingGroupEvent)
    }

    fun getSettingChangeSize(): Int {
        return settingGroupToSettingChangeEvent.size
    }

    fun getDeleteSize(): Int {
        return settingGroupToDeleteEvent.size
    }

    fun getDeleteGroupSize(): Int {
        return settingGroupToDeleteGroupEvent.size
    }

    fun clear() {
        settingGroupToSettingChangeEvent.clear()
        settingGroupToDeleteEvent.clear()
        settingGroupToDeleteGroupEvent.clear()
    }
}
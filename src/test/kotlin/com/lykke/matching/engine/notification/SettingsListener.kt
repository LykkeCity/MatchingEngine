package com.lykke.matching.engine.notification

import com.lykke.matching.engine.daos.setting.AvailableSettingGroup
import com.lykke.matching.engine.services.events.DeleteSettingEvent
import com.lykke.matching.engine.services.events.DeleteSettingGroupEvent
import com.lykke.matching.engine.services.events.SettingChangedEvent
import org.springframework.context.event.EventListener
import java.util.concurrent.ConcurrentHashMap

class SettingsListener() {
    private val settingGroupToSettingChangeEvent = ConcurrentHashMap<AvailableSettingGroup, MutableList<SettingChangedEvent>>()
    private val settingGroupToDeleteEvent = ConcurrentHashMap<AvailableSettingGroup, MutableList<DeleteSettingEvent>>()
    private val settingGroupToDeleteGroupEvent = ConcurrentHashMap<AvailableSettingGroup, MutableList<DeleteSettingGroupEvent>>()

    @EventListener
    fun settingChanged(settingChangedEvent: SettingChangedEvent) {
        val events = settingGroupToSettingChangeEvent.getOrPut(settingChangedEvent.settingGroup) {ArrayList()}
        events!!.add(settingChangedEvent)
    }

    @EventListener
    private fun settingDeleted(deleteSettingEvent: DeleteSettingEvent) {
        val events = settingGroupToDeleteEvent.getOrPut(deleteSettingEvent.settingGroup) {ArrayList()}
        events!!.add(deleteSettingEvent)
    }

    @EventListener
    private fun settingGroupDeleted(deleteSettingGroupEvent: DeleteSettingGroupEvent) {
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
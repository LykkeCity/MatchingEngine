package com.lykke.matching.engine.services.events.listeners

import com.lykke.matching.engine.daos.setting.AvailableSettingGroup
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.services.events.DeleteSettingEvent
import com.lykke.matching.engine.services.events.DeleteSettingGroupEvent
import com.lykke.matching.engine.services.events.SettingChangedEvent
import com.lykke.utils.logging.MetricsLogger
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component
import java.util.logging.Logger
import javax.annotation.PostConstruct

@Component
class MessageProcessingSwitchListener(val applicationSettingsCache: ApplicationSettingsCache) {

    private companion object {
        val LOGGER = Logger.getLogger(MessageProcessingSwitchListener::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()
        val LOG_MESSAGE_FORMAT = "Message processing has been %s, " +
                "by user: %s, comment: %s"
        val START_ACTION = "STARTED"
        val STOP_ACTION = "STOPPED"
    }

    @EventListener
    private fun messageProcessingSwitchChanged(settingChangedEvent: SettingChangedEvent) {
        if (settingChangedEvent.settingGroup != AvailableSettingGroup.MESSAGE_PROCESSING_SWITCH
                || settingChangedEvent.previousSetting == null && !settingChangedEvent.setting.enabled
                || settingChangedEvent.previousSetting?.enabled == settingChangedEvent.setting.enabled) {
            return
        }

        val action =  if (applicationSettingsCache.isMessageProcessingEnabled()) START_ACTION else STOP_ACTION
        val message = getLogMessageMessage(action, settingChangedEvent.user, settingChangedEvent.comment)
        LOGGER.info(message)
        METRICS_LOGGER.logWarning(message)
    }

    @EventListener
    private fun messageProcessingSwitchRemoved(deleteSettingEvent: DeleteSettingEvent) {
        if (deleteSettingEvent.settingGroup != AvailableSettingGroup.MESSAGE_PROCESSING_SWITCH
                || !deleteSettingEvent.deletedSetting.enabled) {
            return
        }

        val message = getLogMessageMessage(START_ACTION, deleteSettingEvent.user, deleteSettingEvent.comment)
        LOGGER.info(message)
        METRICS_LOGGER.logWarning(message)
    }

    @EventListener
    private fun messageProcessingSwtichGroupRemoved(deleteSettingGroupEvent: DeleteSettingGroupEvent) {
        if (deleteSettingGroupEvent.settingGroup != AvailableSettingGroup.MESSAGE_PROCESSING_SWITCH ||
                deleteSettingGroupEvent.deletedSettings.find { it.enabled } == null) {
            return
        }

        val message = getLogMessageMessage(START_ACTION, deleteSettingGroupEvent.user, deleteSettingGroupEvent.comment)
        LOGGER.info(message)
        METRICS_LOGGER.logWarning(message)
    }

    @PostConstruct
    private fun logInitialSwitchStatus() {
        if (!applicationSettingsCache.isMessageProcessingEnabled()) {
            val message = "ME started with message processing DISABLED, all incoming messages will be rejected " +
                    "for enabling message processing change setting group \"${AvailableSettingGroup.MESSAGE_PROCESSING_SWITCH.settingGroupName}\""
            LOGGER.info(message)
            METRICS_LOGGER.logWarning(message)
        }
    }

    private fun getLogMessageMessage(action: String, user: String, comment: String): String {
        return LOG_MESSAGE_FORMAT.format(action, user, comment)
    }
}
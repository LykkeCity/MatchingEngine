package com.lykke.matching.engine.services.events.listeners

import com.lykke.matching.engine.daos.setting.AvailableSettingGroup
import com.lykke.matching.engine.holders.ApplicationSettingsHolder
import com.lykke.matching.engine.services.events.ApplicationSettingDeletedEvent
import com.lykke.matching.engine.services.events.ApplicationGroupDeletedEvent
import com.lykke.matching.engine.services.events.ApplicationSettingCreatedOrUpdatedEvent
import com.lykke.utils.logging.MetricsLogger
import org.slf4j.LoggerFactory
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component
import javax.annotation.PostConstruct

@Component
class MessageProcessingSwitchListener(val applicationSettingsHolder: ApplicationSettingsHolder) {

    private companion object {
        val LOGGER = LoggerFactory.getLogger(MessageProcessingSwitchListener::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()
        val LOG_MESSAGE_FORMAT = "Message processing has been %s, " +
                "by user: %s, comment: %s"
        val START_ACTION = "STARTED"
        val STOP_ACTION = "STOPPED"
    }

    @EventListener
    private fun messageProcessingSwitchChanged(settingChangedEvent: ApplicationSettingCreatedOrUpdatedEvent) {
        if (settingChangedEvent.settingGroup != AvailableSettingGroup.MESSAGE_PROCESSING_SWITCH
                || settingChangedEvent.previousSetting == null && !settingChangedEvent.setting.enabled
                || settingChangedEvent.previousSetting?.enabled == settingChangedEvent.setting.enabled) {
            return
        }

        val action =  if (applicationSettingsHolder.isMessageProcessingEnabled()) START_ACTION else STOP_ACTION
        val message = getLogMessageMessage(action, settingChangedEvent.user, settingChangedEvent.comment)
        LOGGER.info(message)
        METRICS_LOGGER.logWarning(message)
    }

    @EventListener
    private fun messageProcessingSwitchRemoved(deleteSettingEvent: ApplicationSettingDeletedEvent) {
        if (deleteSettingEvent.settingGroup != AvailableSettingGroup.MESSAGE_PROCESSING_SWITCH
                || !deleteSettingEvent.deletedSetting.enabled) {
            return
        }

        val message = getLogMessageMessage(START_ACTION, deleteSettingEvent.user, deleteSettingEvent.comment)
        LOGGER.info(message)
        METRICS_LOGGER.logWarning(message)
    }

    @EventListener
    private fun messageProcessingSwitchGroupRemoved(deleteSettingGroupEvent: ApplicationGroupDeletedEvent) {
        if (deleteSettingGroupEvent.settingGroup != AvailableSettingGroup.MESSAGE_PROCESSING_SWITCH ||
                !deleteSettingGroupEvent.deletedSettings.any { it.enabled }) {
            return
        }

        val message = getLogMessageMessage(START_ACTION, deleteSettingGroupEvent.user, deleteSettingGroupEvent.comment)
        LOGGER.info(message)
        METRICS_LOGGER.logWarning(message)
    }

    @PostConstruct
    private fun logInitialSwitchStatus() {
        if (!applicationSettingsHolder.isMessageProcessingEnabled()) {
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
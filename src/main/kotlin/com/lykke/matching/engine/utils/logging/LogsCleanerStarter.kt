package com.lykke.matching.engine.utils.logging

import com.lykke.matching.engine.LOGGER
import com.lykke.matching.engine.utils.config.Config
import com.lykke.utils.AppInitializer
import com.lykke.utils.files.clean.LogFilesCleaner
import com.lykke.utils.files.clean.config.LogFilesCleanerConfig

fun startLogsCleaner(config: Config) {
    try {
        val logFilesCleanerConfig = config.me.logFilesCleaner
        val logFilesCleanerConfigWithDefaults = LogFilesCleanerConfig(logFilesCleanerConfig.enabled,
                logFilesCleanerConfig.directory,
                logFilesCleanerConfig.period,
                logFilesCleanerConfig.connectionString ?: config.me.db.messageLogConnString,
                logFilesCleanerConfig.blobContainerName,
                logFilesCleanerConfig.uploadDaysThreshold,
                logFilesCleanerConfig.archiveDaysThreshold)

        LogFilesCleaner.start(logFilesCleanerConfigWithDefaults)
    } catch (e: Exception) {
        AppInitializer.teeLog("Unable to start log files cleaner: ${e.message}")
        LOGGER.error(null, e)
    }
}
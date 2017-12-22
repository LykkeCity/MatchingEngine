package com.lykke.matching.engine.database.file

import com.lykke.matching.engine.database.ProcessedMessagesDatabaseAccessor
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import org.nustaq.serialization.FSTConfiguration
import java.io.File
import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.StandardOpenOption
import java.text.SimpleDateFormat
import java.util.Date
import java.util.LinkedList

class FileProcessedMessagesDatabaseAccessor(private val messagesDir: String): ProcessedMessagesDatabaseAccessor {

    companion object {
        val LOGGER = ThrottlingLogger.getLogger(FileProcessedMessagesDatabaseAccessor::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()

        val DATE_FORMAT = SimpleDateFormat("yyyyMMdd")
    }

    private var conf = FSTConfiguration.createDefaultConfiguration()

    override fun loadProcessedMessages(startDate: Date): List<ProcessedMessage> {
        val result = LinkedList<ProcessedMessage>()
        val startFileName = DATE_FORMAT.format(startDate)

        try {
            val dir = File(messagesDir)
            if (dir.exists()) {
                dir.listFiles().forEach { file ->
                    if (!file.isDirectory && file.name >= startFileName) {
                        try {
                            result.addAll(loadFile(file))
                        } catch (e: Exception) {
                            LOGGER.error("Unable to read processed messages file ${file.name}.", e)
                        }
                    }
                }
            } else {
                dir.mkdir()
            }
        } catch(e: Exception) {
            LOGGER.error("Unable to processed messages", e)
            METRICS_LOGGER.logError( "Unable to processed messages", e)
        }

        LOGGER.info("Loaded ${result.size} processed messages")
        return result
    }

    private fun loadFile(file: File): List<ProcessedMessage> {
        val result = LinkedList<ProcessedMessage>()
        val fileLocation = file.toPath()
        Files.readAllLines(fileLocation).forEach {
            val readCase = conf.asObject(it.toByteArray())
            if (readCase is ProcessedMessage) {
                result.add(readCase)
            }
        }
        return result
    }

    override fun saveProcessedMessage(message: ProcessedMessage) {
        try {
            val fileName = DATE_FORMAT.format(Date(message.timestamp))
            val file = File("$messagesDir/$fileName")
            if (!file.exists()) {
                file.createNewFile()
            }
            val bytes = conf.asByteArray(message)
            Files.write(FileSystems.getDefault().getPath("$messagesDir/$fileName"), bytes, StandardOpenOption.CREATE, StandardOpenOption.APPEND)
        } catch (ex: Exception) {
            LOGGER.error("Unable to save message info: $message", ex)
            METRICS_LOGGER.logError( "Unable to save message info: $message", ex)
        }
    }
}
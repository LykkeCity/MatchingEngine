package com.lykke.matching.engine.database.file

import com.lykke.matching.engine.database.ProcessedMessagesDatabaseAccessor
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.utils.config.Config
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import org.apache.commons.lang3.StringUtils
import org.nustaq.serialization.FSTConfiguration
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import java.io.File
import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.StandardOpenOption
import java.text.SimpleDateFormat
import java.util.Arrays
import java.util.Date
import java.util.LinkedList
import java.util.stream.Collectors

class FileProcessedMessagesDatabaseAccessor constructor (private val filePath: String): ProcessedMessagesDatabaseAccessor {

    companion object {
        val LOGGER = ThrottlingLogger.getLogger(FileProcessedMessagesDatabaseAccessor::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()

        val DATE_FORMAT = SimpleDateFormat("yyyyMMdd")
    }

    private var conf = FSTConfiguration.createJsonConfiguration()

    init {
        val messageDir = File(filePath)
        messageDir.mkdirs()
    }

    override fun loadProcessedMessages(startDate: Date): List<ProcessedMessage> {
        var result = LinkedList<ProcessedMessage>()
        val startFileName = DATE_FORMAT.format(startDate)

        try {
            val dir = File(filePath)
            result = Arrays.stream(dir.listFiles())
                    .filter({ !it.isDirectory && it.name >= startFileName })
                    .map { loadFile(it) }
                    .flatMap({ it.stream() })
                    .collect(Collectors.toCollection({ LinkedList<ProcessedMessage>() }))
        } catch(e: Exception) {
            LOGGER.error("Unable to processed messages", e)
            METRICS_LOGGER.logError( "Unable to processed messages", e)
        }

        LOGGER.info("Loaded ${result.size} processed messages")
        return result
    }

    private fun loadFile(file: File): List<ProcessedMessage> {
        try {
            return Files.readAllLines(file.toPath())
                .stream()
                .filter(StringUtils::isNotBlank)
                .map { conf.asObject(it.toByteArray()) }
                .filter({ it is ProcessedMessage })
                .map {it as ProcessedMessage}
                .collect(Collectors.toCollection({ LinkedList<ProcessedMessage>() }))
        } catch (e: Exception) {
            LOGGER.error("Unable to read processed messages file ${file.name}.", e)
        }

        return LinkedList()
    }

    override fun saveProcessedMessage(message: ProcessedMessage) {
        try {
            val fileName = DATE_FORMAT.format(Date(message.timestamp))
            val file = File("$filePath/$fileName")
            if (!file.exists()) {
                file.createNewFile()
            }
            val bytes = conf.asJsonString(message)
            Files.write(FileSystems.getDefault().getPath("$filePath/$fileName"), Arrays.asList(bytes), StandardOpenOption.CREATE, StandardOpenOption.APPEND)
        } catch (ex: Exception) {
            LOGGER.error("Unable to save message info: $message", ex)
            METRICS_LOGGER.logError( "Unable to save message info: $message", ex)
        }
    }
}
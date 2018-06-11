package com.lykke.matching.engine.database.file

import com.lykke.matching.engine.database.ProcessedMessagesDatabaseAccessor
import com.lykke.matching.engine.database.ReadOnlyProcessedMessagesDatabaseAccessor
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.deduplication.ProcessedMessageUtils
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import org.apache.commons.lang3.StringUtils
import org.nustaq.serialization.FSTConfiguration
import java.io.File
import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.StandardOpenOption
import java.text.SimpleDateFormat
import java.time.Duration
import java.time.LocalDate
import java.time.ZoneId
import java.util.Arrays
import java.util.Date
import java.util.LinkedList
import java.util.stream.Collectors

class FileProcessedMessagesDatabaseAccessor constructor (private val filePath: String,
                                                         private val interval: Long): ProcessedMessagesDatabaseAccessor,
        ReadOnlyProcessedMessagesDatabaseAccessor {

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

    override fun get(): Set<ProcessedMessage> {
        val startFileName = DATE_FORMAT.format(getStartDate(interval))

        val result = try {
            val dir = File(filePath)
            Arrays.stream(dir.listFiles())
                    .filter({ !it.isDirectory && it.name >= startFileName })
                    .map { loadFile(it) }
                    .flatMap({ it.stream() })
                    .collect(Collectors.toSet())
        } catch(e: Exception) {
            LOGGER.error("Unable to processed messages", e)
            METRICS_LOGGER.logError( "Unable to processed messages", e)
            return HashSet()
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
        if (ProcessedMessageUtils.isDeduplicationNotNeeded(message.type)) {
            return
        }

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

    private fun getStartDate(interval: Long): Date {
        val days = interval / Duration.ofDays(1).toMillis()  + 1

        return Date.from(LocalDate
                .now()
                .minusDays(days)
                .atStartOfDay(ZoneId.of("UTC"))
                .toInstant())
    }
}
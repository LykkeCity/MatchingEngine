package com.lykke.matching.engine.holders

import com.lykke.matching.engine.utils.uuid.UUIDGenerator
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Value
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.util.concurrent.ConcurrentLinkedQueue

@Component
class UUIDHolderImpl(@Value("\${uuid.holder.size.default}")
                     defaultTargetSize: Int,
                     @Value("\${uuid.holder.size.max}")
                     private val maxTargetSize: Int,
                     private val uuidGenerator: UUIDGenerator) : UUIDHolder {

    companion object {
        private val LOGGER = Logger.getLogger(UUIDHolderImpl::class.java.name)
    }

    private val uuids = ConcurrentLinkedQueue<String>()

    @Volatile
    private var emptyCacheCounter = 0

    private var targetSize = defaultTargetSize

    override fun getNextValue(): String {
        val result = uuids.poll()
        if (result == null) {
            emptyCacheCounter++
            return generateUUID()
        }
        return result
    }

    private fun calculateTargetSize(emptyCacheCount: Int,
                                    currentTargetSize: Int): Int {
        if (emptyCacheCount == 0) {
            return currentTargetSize
        }
        LOGGER.debug("There were cases of empty cache (count: $emptyCacheCount)")
        if (currentTargetSize == maxTargetSize) {
            LOGGER.debug("targetSize is max")
            return currentTargetSize
        }
        val targetSize = currentTargetSize + emptyCacheCount
        if (targetSize > maxTargetSize) {
            LOGGER.debug("Using max target size (expected: $targetSize, max: $maxTargetSize)")
            return maxTargetSize
        }
        return targetSize
    }

    private fun generateUUID() = uuidGenerator.generate()

    @Scheduled(fixedDelayString = "\${uuid.holder.update.interval}")
    private fun generateValues() {
        val emptyCacheCount = emptyCacheCounter
        targetSize = calculateTargetSize(emptyCacheCount, targetSize)
        val currentSize = uuids.size
        (0 until targetSize - currentSize).forEach {
            uuids.add(generateUUID())
        }
        emptyCacheCounter = 0
    }

    @Scheduled(fixedDelayString = "\${uuid.holder.debug.interval}")
    private fun logDebugInfo() {
        LOGGER.debug("Current size: ${uuids.size}, target size: $targetSize")
    }
}
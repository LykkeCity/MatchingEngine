package com.lykke.matching.engine.holders

import com.lykke.matching.engine.database.MessageSequenceNumberDatabaseAccessor
import org.springframework.stereotype.Component

@Component
class MessageSequenceNumberHolder(messageSequenceNumberDatabaseAccessor: MessageSequenceNumberDatabaseAccessor) {

    private var sequenceNumber = messageSequenceNumberDatabaseAccessor.getSequenceNumber()
    private var persistedSequenceNumber = sequenceNumber

    @Synchronized
    fun getNewValue() = ++sequenceNumber

    @Synchronized
    fun getValueToPersist(): Long? {
        return if (persistedSequenceNumber != sequenceNumber) {
            persistedSequenceNumber = sequenceNumber
            sequenceNumber
        } else {
            null
        }
    }

}
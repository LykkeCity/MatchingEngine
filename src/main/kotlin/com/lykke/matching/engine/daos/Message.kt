package com.lykke.matching.engine.daos

import java.util.Date

/** Loggable message */
data class Message(
        val sequenceNumber: Long?,
        val messageId: String,
        val requestId: String,
        val type: String,
        val timestamp: Date,
        val message: String
)
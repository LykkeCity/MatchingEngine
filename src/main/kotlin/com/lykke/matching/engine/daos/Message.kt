package com.lykke.matching.engine.daos

import java.util.Date

/** Loggable message */
data class Message(
        val id: String,
        val type: String,
        val timestamp: Date,
        val message: String
)
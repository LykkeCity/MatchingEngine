package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.Message

interface MessageLogDatabaseAccessor {
    fun log(message: Message)
}
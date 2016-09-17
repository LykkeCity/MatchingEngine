package com.lykke.matching.engine.database

import java.util.Date

interface SharedDatabaseAccessor {
    fun updateKeepAlive(date: Date)
}
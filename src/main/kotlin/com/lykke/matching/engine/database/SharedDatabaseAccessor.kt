package com.lykke.matching.engine.database

import java.util.Date

interface SharedDatabaseAccessor {
    fun getLastKeepAlive(): Date?
    fun updateKeepAlive(date: Date, note: String?)
}
package com.lykke.services.keepalive

import java.util.Date

interface KeepAliveAccessor {
    fun updateKeepAlive(date: Date, service: String, version: String)
}
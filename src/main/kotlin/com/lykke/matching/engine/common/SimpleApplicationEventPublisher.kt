package com.lykke.matching.engine.common

interface SimpleApplicationEventPublisher<T> {
    fun publishEvent(event: T)
}
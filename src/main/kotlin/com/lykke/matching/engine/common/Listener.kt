package com.lykke.matching.engine.common

interface Listener<T> {
    fun onEvent(event: T)
}
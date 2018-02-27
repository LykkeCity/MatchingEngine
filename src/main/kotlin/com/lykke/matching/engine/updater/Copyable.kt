package com.lykke.matching.engine.updater

interface Copyable {
    fun copy(): Copyable
    fun applyToOrigin(origin: Copyable)
}
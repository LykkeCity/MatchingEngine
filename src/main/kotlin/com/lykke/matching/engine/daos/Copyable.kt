package com.lykke.matching.engine.daos

interface Copyable {
    fun copy(): Copyable
    fun applyToOrigin(origin: Copyable)
}
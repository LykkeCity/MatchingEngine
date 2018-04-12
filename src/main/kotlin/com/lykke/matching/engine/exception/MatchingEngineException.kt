package com.lykke.matching.engine.exception

abstract class MatchingEngineException(message: String) : Exception(message) {
    override val message: String
        get() = super.message!!
}
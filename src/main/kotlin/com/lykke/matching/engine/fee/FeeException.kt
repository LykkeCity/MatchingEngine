package com.lykke.matching.engine.fee

open class FeeException(message: String): Exception(message) {
    override val message: String
        get() = super.message!!
}
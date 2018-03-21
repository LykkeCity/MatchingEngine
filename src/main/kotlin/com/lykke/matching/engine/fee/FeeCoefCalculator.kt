package com.lykke.matching.engine.fee

interface FeeCoefCalculator {
    fun calculate(): Double?
}
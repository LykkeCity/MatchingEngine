package com.lykke.matching.engine.outgoing.messages.v2

enum class FeeSizeType(val id: Int) {
    UNKNOWN_FEE_SIZE_TYPE(0),
    PERCENTAGE(1),
    ABSOLUTE(2)
}
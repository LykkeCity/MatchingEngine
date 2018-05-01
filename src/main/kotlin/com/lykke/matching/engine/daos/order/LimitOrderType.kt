package com.lykke.matching.engine.daos.order

enum class LimitOrderType(val externalId: Int) {
    LIMIT(0),
    STOP_LIMIT(1);

    companion object {
        fun getByExternalId(externalId: Int): LimitOrderType {
            return values().firstOrNull { it.externalId == externalId }
                    ?: throw IllegalArgumentException("LimitOrderType (externalId=$externalId) is not found")
        }
    }
}
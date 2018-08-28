package com.lykke.matching.engine.daos.order

enum class OrderTimeInForce(val externalId: Int) {
    /** Good till cancel */
    GTC(0),
    /** Good till date */
    GTD(1),
    /** Immediate or Cancel */
    IOC(2);

    companion object {
        fun getByExternalId(externalId: Int): OrderTimeInForce {
            return values().firstOrNull { it.externalId == externalId }
                    ?: throw IllegalArgumentException("OrderTimeInForce (externalId=$externalId) is not found")
        }
    }
}
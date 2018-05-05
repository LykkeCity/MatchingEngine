package com.lykke.matching.engine.order

enum class OrderCancelMode(val externalId: Int) {

    NOT_EMPTY_SIDE(0),
    BOTH_SIDES(1),
    SELL_SIDE(2),
    BUY_SIDE(3);

    companion object {
        fun getByExternalId(externalId: Int): OrderCancelMode {
            OrderCancelMode.values()
                    .filter { it.externalId == externalId }
                    .forEach { return it }
            throw IllegalArgumentException("OrderCancelMode (externalId=$externalId) is not found")
        }
    }

}
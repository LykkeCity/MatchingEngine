package com.lykke.matching.engine.outgoing.messages

import java.math.BigDecimal
import java.util.Date

class BalanceUpdate( val id: String,
                     val type: String,
                     val timestamp: Date,
                     val balances: List<ClientBalanceUpdate>): JsonSerializable() {

    override fun toString(): String {
        return toJson()
    }
}

class ClientBalanceUpdate(
    val id: String,
    val asset: String,
    val oldBalance: BigDecimal,
    var newBalance: BigDecimal,
    val oldReserved: BigDecimal,
    var newReserved: BigDecimal
)
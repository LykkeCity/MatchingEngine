package com.lykke.matching.engine.outgoing.messages

import java.util.Date

class BalanceUpdate( val id: String,
                     val type: String,
                     val timestamp: Date,
                     var balances: List<ClientBalanceUpdate>): JsonSerializable() {

    override fun toString(): String {
        return toJson()
    }
}

class ClientBalanceUpdate(
    val id: String,
    val asset: String,
    val oldBalance: Double,
    var newBalance: Double,
    val oldReserved: Double,
    var newReserved: Double
)
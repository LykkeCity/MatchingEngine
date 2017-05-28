package com.lykke.matching.engine.outgoing.messages

import com.google.gson.GsonBuilder
import java.util.Date

class BalanceUpdate( val id: String,
                     val type: String,
                     val timestamp: Date,
                     val balances: List<ClientBalanceUpdate>): JsonSerializable() {

    override fun toJson(): String {
        return GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ").create().toJson(this)
    }

    override fun toString(): String {
        return toJson()
    }
}

class ClientBalanceUpdate(
    val id: String,
    val asset: String,
    val oldBalance: Double,
    var newBalance: Double
)
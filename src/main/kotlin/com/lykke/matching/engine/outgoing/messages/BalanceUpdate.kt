package com.lykke.matching.engine.outgoing.messages

import com.lykke.matching.engine.AppContext
import com.lykke.matching.engine.database.WalletsStorage
import java.util.Date

class BalanceUpdate( val id: String,
                     val type: String,
                     val timestamp: Date,
                     val balances: List<ClientBalanceUpdate>): JsonSerializable() {

    val persisted: Boolean = AppContext.getWalletsStorage() == WalletsStorage.Azure

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
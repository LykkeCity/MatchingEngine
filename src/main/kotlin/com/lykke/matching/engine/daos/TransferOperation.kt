package com.lykke.matching.engine.daos

import com.lykke.matching.engine.utils.NumberUtils
import com.lykke.matching.engine.daos.v2.FeeInstruction
import java.math.BigDecimal
import java.util.Date

data class TransferOperation(
        val id: String,
        val externalId: String,
        val fromClientId: String,
        val toClientId: String,
        val asset: String,
        val dateTime: Date,
        val volume: BigDecimal,
        val overdraftLimit: BigDecimal?,
        val fees: List<FeeInstruction>?) {

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as TransferOperation

        if (id != other.id) return false
        if (externalId != other.externalId) return false
        if (fromClientId != other.fromClientId) return false
        if (toClientId != other.toClientId) return false
        if (asset != other.asset) return false
        if (dateTime != other.dateTime) return false
        if (!NumberUtils.equalsIgnoreScale(volume, other.volume)) return false
        if (!NumberUtils.equalsIgnoreScale(overdraftLimit, other.overdraftLimit)) return false
        if (fees != other.fees) return false

        return true
    }

    override fun hashCode(): Int {
        var result = id.hashCode()
        result = 31 * result + externalId.hashCode()
        result = 31 * result + fromClientId.hashCode()
        result = 31 * result + toClientId.hashCode()
        result = 31 * result + asset.hashCode()
        result = 31 * result + dateTime.hashCode()
        result = 31 * result + volume.stripTrailingZeros().hashCode()
        result = 31 * result + (overdraftLimit?.stripTrailingZeros()?.hashCode() ?: 0)
        result = 31 * result + (fees?.hashCode() ?: 0)
        return result
    }
}
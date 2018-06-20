package com.lykke.matching.engine.daos

import com.lykke.matching.engine.utils.NumberUtils
import java.math.BigDecimal
import java.util.Date

data class WalletOperation(
        val id: String,
        val externalId: String?,
        val clientId: String,
        val assetId: String,
        val dateTime: Date,
        val amount: BigDecimal,
        val reservedAmount: BigDecimal = BigDecimal.ZERO,
        val isFee: Boolean = false
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as WalletOperation

        if (id != other.id) return false
        if (externalId != other.externalId) return false
        if (clientId != other.clientId) return false
        if (assetId != other.assetId) return false
        if (dateTime != other.dateTime) return false
        if (!NumberUtils.equalsIgnoreScale(amount, other.amount)) return false
        if (!NumberUtils.equalsIgnoreScale(reservedAmount, other.reservedAmount)) return false
        if (isFee != other.isFee) return false

        return true
    }

    override fun hashCode(): Int {
        var result = id.hashCode()
        result = 31 * result + (externalId?.hashCode() ?: 0)
        result = 31 * result + clientId.hashCode()
        result = 31 * result + assetId.hashCode()
        result = 31 * result + dateTime.hashCode()
        result = 31 * result + amount.stripTrailingZeros().hashCode()
        result = 31 * result + reservedAmount.stripTrailingZeros().hashCode()
        result = 31 * result + isFee.hashCode()
        return result
    }
}
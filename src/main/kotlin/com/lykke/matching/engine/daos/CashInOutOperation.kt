package com.lykke.matching.engine.daos

import com.lykke.matching.engine.daos.fee.v2.NewFeeInstruction
import com.lykke.matching.engine.utils.NumberUtils
import java.math.BigDecimal
import java.util.*

data class CashInOutOperation(
        val matchingEngineOperationId: String,
        val externalId: String?,
        val clientId: String,
        val asset: Asset?,
        val dateTime: Date,
        val amount: BigDecimal,
        val feeInstructions: List<NewFeeInstruction>) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as CashInOutOperation

        if (matchingEngineOperationId != other.matchingEngineOperationId) return false
        if (externalId != other.externalId) return false
        if (clientId != other.clientId) return false
        if (asset != other.asset) return false
        if (dateTime != other.dateTime) return false
        if (!NumberUtils.equalsIgnoreScale(amount, other.amount)) return false
        if (feeInstructions != other.feeInstructions) return false
        return true
    }

    override fun hashCode(): Int {
        var result = matchingEngineOperationId.hashCode()
        result = 31 * result + (externalId?.hashCode() ?: 0)
        result = 31 * result + clientId.hashCode()
        result = 31 * result + (asset?.hashCode() ?: 0)
        result = 31 * result + dateTime.hashCode()
        result = 31 * result + amount.stripTrailingZeros().hashCode()
        result = 31 * result + feeInstructions.hashCode()
        return result
    }
}
package com.lykke.matching.engine.daos

import com.lykke.matching.engine.utils.RoundingUtils
import java.math.BigDecimal
import java.util.Date

data class FeeTransfer(
        val externalId: String?,
        val fromClientId: String,
        val toClientId: String,
        val dateTime: Date,
        val volume: BigDecimal,
        val asset: String,
        val feeCoef: BigDecimal?
) {
      override fun equals(other: Any?): Boolean {
          if (this === other) return true
          if (javaClass != other?.javaClass) return false

          other as FeeTransfer

          if (externalId != other.externalId) return false
          if (fromClientId != other.fromClientId) return false
          if (toClientId != other.toClientId) return false
          if (dateTime != other.dateTime) return false
          if (RoundingUtils.equalsIgnoreScale(volume, other.volume)) return false
          if (asset != other.asset) return false
          if (RoundingUtils.equalsIgnoreScale(feeCoef, other.feeCoef)) return false

          return true
      }

      override fun hashCode(): Int {
          var result = externalId?.hashCode() ?: 0
          result = 31 * result + fromClientId.hashCode()
          result = 31 * result + toClientId.hashCode()
          result = 31 * result + dateTime.hashCode()
          result = 31 * result + volume.stripTrailingZeros().hashCode()
          result = 31 * result + asset.hashCode()
          result = 31 * result + (feeCoef?.stripTrailingZeros()?.hashCode() ?: 0)
          return result
      }
  }
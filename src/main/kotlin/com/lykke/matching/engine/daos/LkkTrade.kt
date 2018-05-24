package com.lykke.matching.engine.daos

import com.lykke.matching.engine.utils.RoundingUtils
import java.math.BigDecimal
import java.util.Date

data class LkkTrade(
        val assetPair: String,
        val clientId: String,
        val price: BigDecimal,
        val volume: BigDecimal,
        val date: Date


) {
     override fun equals(other: Any?): Boolean {
         if (this === other) return true
         if (javaClass != other?.javaClass) return false

         other as LkkTrade

         if (assetPair != other.assetPair) return false
         if (clientId != other.clientId) return false
         if (!RoundingUtils.equalsIgnoreScale(price, other.price)) return false
         if (!RoundingUtils.equalsIgnoreScale(volume, other.volume)) return false
         if (date != other.date) return false

         return true
     }

     override fun hashCode(): Int {
         var result = assetPair.hashCode()
         result = 31 * result + clientId.hashCode()
         result = 31 * result + price.stripTrailingZeros().hashCode()
         result = 31 * result + volume.stripTrailingZeros().hashCode()
         result = 31 * result + date.hashCode()
         return result
     }
 }
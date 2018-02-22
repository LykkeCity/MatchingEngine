package com.lykke.matching.engine.daos.wallet

import com.lykke.matching.engine.exception.BalanceException
import com.lykke.matching.engine.updater.Copyable

class AssetBalance private constructor(val asset: String,
                                       var balance: Double,
                                       var reserved: Double,
                                       private val isCopy: Boolean) : Copyable {

    constructor(asset: String,
                balance: Double = 0.0,
                reserved: Double = 0.0): this(asset, balance, reserved, false)

    // Origin values are needed to new values validation
    val originBalance: Double? = if (isCopy) balance else null
    val originReserved: Double? = if (isCopy) reserved else null

    override fun copy(): AssetBalance {
        return AssetBalance(asset, balance, reserved, true)
    }

    override fun applyToOrigin(origin: Copyable) {
        origin as AssetBalance
        origin.balance = balance
        origin.reserved = reserved
    }

    fun validate() {
        val balanceInfo = "Invalid balance (asset=$asset, balance=$balance, reserved=$reserved" + (if (isCopy) ", previous=$originBalance, previous reserved=$originReserved" else "") + ")"

        // Balance can become negative earlier due to transfer operation with overdraftLimit > 0.
        // In this case need to check only difference of reserved & main balance.
        // It shouldn't be greater than previous one.
        if (balance < 0.0 && !(isCopy && originBalance!! < 0.0 && (originBalance >= balance || originReserved!! - originBalance >= reserved - balance))) {
            throw BalanceException(balanceInfo)
        }
        if (reserved < 0.0 && !(isCopy && originReserved!! <= reserved)) {
            throw BalanceException(balanceInfo)
        }
        if (balance < reserved && !(isCopy && (originReserved!! - originBalance!! >= reserved - balance))) {
            throw BalanceException(balanceInfo)
        }
    }
}

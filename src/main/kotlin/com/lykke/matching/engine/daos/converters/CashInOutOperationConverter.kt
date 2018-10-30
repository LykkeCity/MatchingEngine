package com.lykke.matching.engine.daos.converters

import com.lykke.matching.engine.daos.CashInOutOperation
import com.lykke.matching.engine.daos.WalletOperation
import java.math.BigDecimal

class CashInOutOperationConverter {
    companion object {
        fun fromCashInOutOperationToWalletOperation(cashInOutOperation: CashInOutOperation): WalletOperation {
            return WalletOperation(cashInOutOperation.clientId,
                    cashInOutOperation.asset!!.assetId,
                    cashInOutOperation.amount,
                    BigDecimal.ZERO)
        }
    }
}
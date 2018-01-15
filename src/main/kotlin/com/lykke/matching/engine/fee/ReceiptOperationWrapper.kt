package com.lykke.matching.engine.fee

import com.lykke.matching.engine.daos.WalletOperation

class ReceiptOperationWrapper(receiptOperation: WalletOperation) {
    val baseReceiptOperation = receiptOperation
    var currentReceiptOperation = receiptOperation
}
package com.lykke.matching.engine.daos.context

import com.lykke.matching.engine.daos.TransferOperation
import com.lykke.matching.engine.deduplication.ProcessedMessage

data class CashTransferContext(
        val messageId: String,
        val transferOperation: TransferOperation,
        val processedMessage: ProcessedMessage)
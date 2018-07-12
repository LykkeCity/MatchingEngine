package com.lykke.matching.engine.daos.context

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.TransferOperation
import com.lykke.matching.engine.daos.fee.v2.NewFeeInstruction
import com.lykke.matching.engine.daos.v2.FeeInstruction
import com.lykke.matching.engine.deduplication.ProcessedMessage
import java.util.*

class CashTransferContext(
        val messageId: String,
        val feeInstruction: FeeInstruction?,
        val feeInstructions: List<NewFeeInstruction>,
        val transferOperation: TransferOperation,
        val asset: Asset?,
        val inputAssetId: String,
        val processedMessage: ProcessedMessage,
        val operationStartTime: Date)
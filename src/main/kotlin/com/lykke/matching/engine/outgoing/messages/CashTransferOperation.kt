package com.lykke.matching.engine.outgoing.messages

import com.lykke.matching.engine.daos.FeeInstruction
import com.lykke.matching.engine.daos.FeeTransfer
import com.lykke.matching.engine.daos.fee.Fee
import java.util.Date

class CashTransferOperation(
        val id: String,
        val fromClientId: String,
        val toClientId: String,
        val dateTime: Date,
        val volume: String,
        val overdraftLimit: Double?,
        var asset: String,
        val feeInstruction: FeeInstruction?,
        val feeTransfer: FeeTransfer?,
        val fees: List<Fee>?): JsonSerializable()
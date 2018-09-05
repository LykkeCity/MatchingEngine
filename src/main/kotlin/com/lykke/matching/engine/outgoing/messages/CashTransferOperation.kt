package com.lykke.matching.engine.outgoing.messages

import com.lykke.matching.engine.daos.v2.FeeInstruction
import com.lykke.matching.engine.daos.FeeTransfer
import com.lykke.matching.engine.daos.fee.v2.Fee
import java.math.BigDecimal
import java.util.Date

class CashTransferOperation(
        val id: String,
        val fromClientId: String,
        val toClientId: String,
        val dateTime: Date,
        val volume: String,
        val overdraftLimit: BigDecimal?,
        var asset: String,
        val feeInstruction: FeeInstruction?,
        val feeTransfer: FeeTransfer?,
        val fees: List<Fee>?,
        val messageId: String)
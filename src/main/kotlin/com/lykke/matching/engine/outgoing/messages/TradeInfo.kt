package com.lykke.matching.engine.outgoing.messages

import com.lykke.matching.engine.daos.FeeInstruction
import com.lykke.matching.engine.daos.FeeTransfer
import java.util.Date

class TradeInfo(
        val marketClientId: String,
        val marketVolume: String,
        val marketAsset: String,
        val limitClientId: String,
        val limitVolume: String,
        val limitAsset: String,
        val price: Double,
        val limitOrderId: String,
        val limitOrderExternalId: String,
        val timestamp: Date,
        val feeInstruction: FeeInstruction?,
        val feeTransfer: FeeTransfer?,
        val feeInstructions: List<FeeInstruction>?,
        val feeTransfers: List<FeeTransfer>?
)
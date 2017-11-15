package com.lykke.matching.engine.outgoing.messages

import com.lykke.matching.engine.daos.FeeInstruction
import com.lykke.matching.engine.daos.FeeTransfer
import java.util.Date

class LimitTradeInfo(
        val clientId: String,
        val asset: String,
        val volume: String,
        val price: Double,
        val timestamp: Date,

        val oppositeOrderId: String,
        val oppositeOrderExternalId: String,
        val oppositeAsset: String,
        val oppositeClientId: String,
        val oppositeVolume: String,
        val feeInstruction: FeeInstruction?,
        val feeTransfer: FeeTransfer?
)
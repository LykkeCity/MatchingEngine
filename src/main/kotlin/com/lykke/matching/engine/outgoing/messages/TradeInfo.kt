package com.lykke.matching.engine.outgoing.messages

import com.lykke.matching.engine.daos.FeeInstruction
import com.lykke.matching.engine.daos.FeeTransfer
import com.lykke.matching.engine.daos.fee.Fee
import java.math.BigDecimal
import java.util.Date

class TradeInfo(
        val tradeId: String,
        val marketClientId: String,
        val marketVolume: String,
        val marketAsset: String,
        val limitClientId: String,
        val limitVolume: String,
        val limitAsset: String,
        val price: BigDecimal,
        val limitOrderId: String,
        val limitOrderExternalId: String,
        val timestamp: Date,
        val index: Long,
        val feeInstruction: FeeInstruction?,
        val feeTransfer: FeeTransfer?,
        val fees: List<Fee>,
        val absoluteSpread: BigDecimal?,
        val relativeSpread: BigDecimal?
)
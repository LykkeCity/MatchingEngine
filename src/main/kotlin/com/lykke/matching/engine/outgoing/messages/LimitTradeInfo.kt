package com.lykke.matching.engine.outgoing.messages

import com.lykke.matching.engine.daos.v2.FeeInstruction
import com.lykke.matching.engine.daos.FeeTransfer
import com.lykke.matching.engine.daos.fee.v2.Fee
import com.lykke.matching.engine.outgoing.messages.v2.enums.TradeRole
import java.math.BigDecimal
import java.util.Date

class LimitTradeInfo(
        val tradeId: String,
        val clientId: String,
        val asset: String,
        val volume: String,
        val price: BigDecimal,
        val timestamp: Date,

        val oppositeOrderId: String,
        val oppositeOrderExternalId: String,
        val oppositeAsset: String,
        val oppositeClientId: String,
        val oppositeVolume: String,
        val index: Long,
        val feeInstruction: FeeInstruction?,
        val feeTransfer: FeeTransfer?,
        val fees: List<Fee>,
        val absoluteSpread: BigDecimal?,
        val relativeSpread: BigDecimal?,
        @Transient
        val role: TradeRole
)
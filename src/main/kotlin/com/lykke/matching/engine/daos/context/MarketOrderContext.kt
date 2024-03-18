package com.lykke.matching.engine.daos.context

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.v2.FeeInstruction
import com.lykke.matching.engine.daos.MarketOrder
import com.lykke.matching.engine.daos.fee.v2.NewFeeInstruction
import com.lykke.matching.engine.deduplication.ProcessedMessage
import java.math.BigDecimal

class MarketOrderContext(val messageId: String,
                         val assetPair: AssetPair?,
                         val baseAsset: Asset?,
                         val quotingAsset: Asset?,
                         val fee: FeeInstruction?,
                         val fees: List<NewFeeInstruction>,
                         val marketPriceDeviationThreshold: BigDecimal?,
                         val processedMessage: ProcessedMessage,
                         val marketOrder: MarketOrder)
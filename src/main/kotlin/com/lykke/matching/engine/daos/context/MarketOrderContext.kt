package com.lykke.matching.engine.daos.context

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.MarketOrder
import com.lykke.matching.engine.deduplication.ProcessedMessage
import java.math.BigDecimal

class MarketOrderContext(val messageId: String,
                         val assetPair: AssetPair?,
                         val baseAsset: Asset?,
                         val quotingAsset: Asset?,
                         val marketPriceDeviationThreshold: BigDecimal?,
                         val processedMessage: ProcessedMessage,
                         val marketOrder: MarketOrder)
package com.lykke.matching.engine.daos.context

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.MarketOrder
import java.math.BigDecimal

class MarketOrderContext(val assetPair: AssetPair?,
                         val marketPriceDeviationThreshold: BigDecimal?,
                         val marketOrder: MarketOrder)
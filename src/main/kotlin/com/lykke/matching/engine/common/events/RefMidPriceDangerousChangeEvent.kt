package com.lykke.matching.engine.common.events

import java.math.BigDecimal

class RefMidPriceDangerousChangeEvent(val assetPairId: String, val refMidPrice: BigDecimal, val cancel: Boolean)
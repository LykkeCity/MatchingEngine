package com.lykke.matching.engine.daos

import java.io.Serializable
import java.math.BigDecimal

class MidPrice(val assetPairId: String, val midPrice: BigDecimal, val timestamp: Long): Serializable
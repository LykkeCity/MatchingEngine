package com.lykke.matching.engine.daos.wallet

import java.io.Serializable
import java.math.BigDecimal

class AssetBalance(val clientId: String,
                   val asset: String,
                   var balance: BigDecimal = BigDecimal.ZERO,
                   var reserved: BigDecimal = BigDecimal.ZERO) : Serializable
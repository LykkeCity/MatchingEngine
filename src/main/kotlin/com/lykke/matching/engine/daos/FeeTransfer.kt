package com.lykke.matching.engine.daos

import java.math.BigDecimal

class FeeTransfer(val fromClientId: String,
                  val toClientId: String,
                  val volume: BigDecimal,
                  val asset: String,
                  val feeCoef: BigDecimal?)
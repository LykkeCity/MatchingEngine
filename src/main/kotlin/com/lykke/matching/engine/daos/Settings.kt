package com.lykke.matching.engine.daos

import java.math.BigDecimal

data class Settings(val trustedClients: Set<String>,
                    val disabledAssets: Set<String>,
                    val moPriceDeviationThresholds: Map<String, BigDecimal>)
package com.lykke.matching.engine.daos.balance

import java.math.BigDecimal

data class ReservedVolumeCorrection(val clientId: String,
                                    val assetId: String,
                                    val orderIds: String?,
                                    val oldReserved: BigDecimal,
                                    val newReserved: BigDecimal)
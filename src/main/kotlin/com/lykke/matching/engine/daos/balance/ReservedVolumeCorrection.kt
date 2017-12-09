package com.lykke.matching.engine.daos.balance

data class ReservedVolumeCorrection(val clientId: String,
                                    val assetId: String,
                                    val orderIds: String?,
                                    val oldReserved: Double,
                                    val newReserved: Double)
package com.lykke.matching.engine.daos

import java.util.Date

data class SwapOperation(val id: String, val externalId: String, val dateTime: Date,
                         val clientId1: String, val asset1: String, val volume1: Double,
                         val clientId2: String, val asset2: String, val volume2: Double)
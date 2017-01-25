package com.lykke.matching.engine.daos

import java.util.Date

data class WalletOperation(val id: String, val externalId: String?, val clientId: String, val assetId: String, val dateTime: Date, val amount: Double)
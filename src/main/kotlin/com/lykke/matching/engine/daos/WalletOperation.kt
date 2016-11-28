package com.lykke.matching.engine.daos

import java.util.Date

data class WalletOperation(val clientId: String, val uid: String, val assetId: String, val dateTime: Date, val amount: Double, val transactionId: String?)
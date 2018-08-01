package com.lykke.matching.engine.daos.context

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.WalletOperation

data class CashOperationContext(val uid: String,
                           val messageId: String,
                           val walletOperation: WalletOperation,
                           val asset: Asset,
                           val assetDisabled: Boolean,
                           val businessId: String,
                           val clientId: String)
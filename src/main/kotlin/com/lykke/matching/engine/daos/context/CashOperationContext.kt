package com.lykke.matching.engine.daos.context

import com.lykke.matching.engine.daos.WalletOperation

class CashOperationContext(val uid: String,
                           val messageId: String,
                           val walletOperation: WalletOperation,
                           val businessId: String,
                           val clientId: String)
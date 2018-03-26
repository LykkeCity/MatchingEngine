package com.lykke.matching.engine.order.cancel

import com.lykke.matching.engine.daos.WalletOperation

data class TransitionalLimitOrdersCancelResult(val walletOperations: List<WalletOperation>)
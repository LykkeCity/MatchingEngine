package com.lykke.matching.engine.database.common.entity

import com.lykke.matching.engine.daos.wallet.AssetBalance
import com.lykke.matching.engine.daos.wallet.Wallet

class BalancesData(
        val wallets: Collection<Wallet>,
        val balances: Collection<AssetBalance>
)
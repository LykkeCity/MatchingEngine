package com.lykke.matching.engine.database.common

import com.lykke.matching.engine.daos.wallet.AssetBalance
import com.lykke.matching.engine.daos.wallet.Wallet

class PersistenceData(val wallets: Collection<Wallet>,
                      val balances: Collection<AssetBalance>) {

    fun details() = "wallets: ${wallets.size}, balances: ${balances.size}"

}
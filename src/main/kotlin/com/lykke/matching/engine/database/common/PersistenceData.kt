package com.lykke.matching.engine.database.common

import com.lykke.matching.engine.daos.wallet.ClientAssetBalance
import com.lykke.matching.engine.daos.wallet.Wallet

class PersistenceData(val wallets: Collection<Wallet>,
                      val clientAssetBalances: Collection<ClientAssetBalance>) {

    fun details() = "wallets: ${wallets.size}, balances: ${clientAssetBalances.size}"

}
package com.lykke.matching.engine.database.common

import com.lykke.matching.engine.daos.wallet.AssetBalance
import com.lykke.matching.engine.daos.wallet.Wallet
import com.lykke.matching.engine.deduplication.ProcessedMessage

class PersistenceData(val wallets: Collection<Wallet>,
                      val balances: Collection<AssetBalance>,
                      val message: ProcessedMessage? = null) {

    fun details() = "w: ${wallets.size}, b: ${balances.size}, m: ${message?.messageId}"

}
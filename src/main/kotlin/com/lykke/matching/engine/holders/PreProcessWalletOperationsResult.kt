package com.lykke.matching.engine.holders

import com.lykke.matching.engine.daos.CopyWrapper
import com.lykke.matching.engine.daos.wallet.AssetBalance
import com.lykke.matching.engine.daos.wallet.Wallet
import com.lykke.matching.engine.outgoing.messages.ClientBalanceUpdate
import java.util.Date

class PreProcessWalletOperationsResult private constructor(
        val empty: Boolean,
        val balances: Map<String, MutableMap<String, CopyWrapper<AssetBalance>>>?,
        val wallets: Map<String, CopyWrapper<Wallet>>?,
        val clients: Set<String>?,
        val updates: Map<String, ClientBalanceUpdate>?,
        val timestamp: Date?,
        validate: Boolean
) {

    constructor(balances: Map<String, MutableMap<String, CopyWrapper<AssetBalance>>>,
                wallets: Map<String, CopyWrapper<Wallet>>,
                clients: Set<String>,
                updates: Map<String, ClientBalanceUpdate>,
                timestamp: Date?,
                validate: Boolean) :
            this(false, balances, wallets, clients, updates, timestamp, validate)

    constructor(validate: Boolean) : this(true, null, null, null, null, null, validate)

    private fun validate() {
        wallets?.values?.forEach { it.copy.validate() }
        balances?.values?.forEach { it.values.forEach { it.copy.validate() } }
    }

    init {
        if (validate) {
            validate()
        }
    }
}
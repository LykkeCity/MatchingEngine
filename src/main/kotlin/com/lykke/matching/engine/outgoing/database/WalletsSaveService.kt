package com.lykke.matching.engine.outgoing.database

import com.lykke.matching.engine.daos.wallet.Wallet
import com.lykke.matching.engine.database.WalletDatabaseAccessor
import com.lykke.utils.logging.ThrottlingLogger
import java.util.concurrent.BlockingQueue

class WalletsSaveService(private val walletDatabaseAccessor: WalletDatabaseAccessor,
                         private val updatedWalletsQueue: BlockingQueue<List<Wallet>>) : Thread() {
    companion object {
        private val LOGGER = ThrottlingLogger.getLogger(WalletsSaveService::class.java.name)
    }

    override fun run() {
        while (true) {
            try {
                val wallets = updatedWalletsQueue.take()
                if (wallets.size == 1) {
                    walletDatabaseAccessor.insertOrUpdateWallet(wallets.first())
                } else {
                    walletDatabaseAccessor.insertOrUpdateWallets(wallets)
                }
            } catch (e: Exception) {
                LOGGER.error("Unable to save wallets", e)
            }
        }
    }
}
package com.lykke.matching.engine.database.listeners

import com.lykke.matching.engine.daos.wallet.Wallet
import com.lykke.matching.engine.database.WalletDatabaseAccessor
import com.lykke.matching.engine.database.redis.RedisPersistenceManager
import org.apache.log4j.Logger
import java.util.concurrent.BlockingQueue
import javax.annotation.PostConstruct
import kotlin.concurrent.thread

class WalletOperationsPersistListener (private val updatedWalletsQueue: BlockingQueue<Collection<Wallet>>,
                                       private val secondaryBalancesAccessor: WalletDatabaseAccessor?) {
    companion object{
        private val LOGGER = Logger.getLogger(WalletOperationsPersistListener::class.java.name)
    }

    @PostConstruct
    fun init () {
        thread(name = "${RedisPersistenceManager::class.java.name}.asyncBalancesWriter") {
            while (true) {
                try {
                    val wallets = updatedWalletsQueue.take()
                    secondaryBalancesAccessor!!.insertOrUpdateWallets(wallets.toList())
                } catch (e: Exception) {
                    LOGGER.error("Unable to save wallets", e)
                }
            }
        }
    }
}
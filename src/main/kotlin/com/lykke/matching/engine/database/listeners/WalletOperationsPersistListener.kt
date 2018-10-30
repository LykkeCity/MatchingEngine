package com.lykke.matching.engine.database.listeners

import com.lykke.matching.engine.common.QueueConsumer
import com.lykke.matching.engine.database.WalletDatabaseAccessor
import com.lykke.matching.engine.database.reconciliation.events.AccountPersistEvent
import org.apache.log4j.Logger
import java.util.concurrent.BlockingQueue
import javax.annotation.PostConstruct
import kotlin.concurrent.thread

class WalletOperationsPersistListener(private val updatedWalletsQueue: BlockingQueue<AccountPersistEvent>,
                                      private val secondaryBalancesAccessor: WalletDatabaseAccessor) : QueueConsumer<AccountPersistEvent> {
    companion object {
        private val LOGGER = Logger.getLogger(WalletOperationsPersistListener::class.java.name)
    }

    @PostConstruct
    fun init() {
        thread(name = "${WalletOperationsPersistListener::class.java.name}.asyncBalancesWriter") {
            while (true) {
                try {
                    val wallets = updatedWalletsQueue.take().persistenceData
                    secondaryBalancesAccessor.insertOrUpdateWallets(wallets.toList())
                } catch (e: Exception) {
                    LOGGER.error("Unable to save wallets", e)
                }
            }
        }
    }
}
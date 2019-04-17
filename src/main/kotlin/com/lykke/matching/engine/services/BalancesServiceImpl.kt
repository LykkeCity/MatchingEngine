package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.wallet.Wallet
import com.lykke.matching.engine.database.PersistenceManager
import com.lykke.matching.engine.database.common.entity.BalancesData
import com.lykke.matching.engine.database.common.entity.PersistenceData
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.utils.logging.MetricsLogger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

@Service
class BalancesServiceImpl(private val balancesHolder: BalancesHolder,
                          private val persistenceManager: PersistenceManager): BalancesService {
    private companion object {
        val LOGGER = LoggerFactory.getLogger(BalancesServiceImpl::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    /**
     * Persists wallets to the db and updates values in cache,
     * Note: this method will not send balance updates notifications
     */
    override fun insertOrUpdateWallets(wallets: Collection<Wallet>, messageSequenceNumber: Long?): Boolean {
        val updated = persistenceManager.persist(PersistenceData(BalancesData(wallets,
                wallets.flatMap { it.balances.values }),
                null,
                null,
                null,
                messageSequenceNumber = messageSequenceNumber))
        if (!updated) {
            val message = "Can not persist balances data, wallets: ${wallets.size}"
            LOGGER.error(message)
            METRICS_LOGGER.logError(message)
            return false
        }

        balancesHolder.setWallets(wallets)
        return true
    }
}
package com.lykke.matching.engine.balance.util

import com.lykke.matching.engine.balance.WalletOperationsProcessorFactory
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.database.PersistenceManager
import com.lykke.matching.engine.database.common.entity.PersistenceData
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.notification.BalanceUpdateHandlerTest
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import java.math.BigDecimal

class TestBalanceHolderWrapper @Autowired constructor(private val balanceUpdateHandlerTest: BalanceUpdateHandlerTest,
                                                      private val balancesHolder: BalancesHolder,
                                                      private val walletOperationsProcessorFactory: WalletOperationsProcessorFactory,
                                                      private val persistenceManager: PersistenceManager) {

    companion object {
        val logger = Logger.getLogger(TestBalanceHolderWrapper::class.java.name)
    }

    fun updateBalance(clientId: String, assetId: String, balance: Double, validate: Boolean = false) {
        val walletProcessor = walletOperationsProcessorFactory.create(logger, validate)
        val currentBalance = balancesHolder.getBalance(clientId, assetId)
        walletProcessor.preProcess(listOf(WalletOperation(clientId, assetId, BigDecimal.valueOf(balance).minus(currentBalance))))
                .apply()
        persistenceManager.persist(PersistenceData(walletProcessor.persistenceData(), null, null, null, null))
        balanceUpdateHandlerTest.clear()
    }

    fun updateReservedBalance(clientId: String, assetId: String, reservedBalance: Double, validate: Boolean = false) {
        val walletProcessor = walletOperationsProcessorFactory.create(logger, validate)
        val currentReservedBalance = balancesHolder.getReservedBalance(clientId, assetId)

        walletProcessor.preProcess(listOf(WalletOperation(clientId, assetId, BigDecimal.ZERO, BigDecimal.valueOf(reservedBalance).minus(currentReservedBalance))))
                .apply()
        persistenceManager.persist(PersistenceData(walletProcessor.persistenceData(), null, null, null, null))
        balanceUpdateHandlerTest.clear()
    }
}
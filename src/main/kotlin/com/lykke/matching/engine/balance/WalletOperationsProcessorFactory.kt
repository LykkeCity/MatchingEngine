package com.lykke.matching.engine.balance

import com.lykke.matching.engine.holders.ApplicationSettingsHolder
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.order.transaction.CurrentTransactionBalancesHolderFactory
import com.lykke.matching.engine.services.BalancesService
import org.apache.log4j.Logger
import org.springframework.stereotype.Component

@Component
class WalletOperationsProcessorFactory(private val currentTransactionBalancesHolderFactory: CurrentTransactionBalancesHolderFactory,
                                       private val applicationSettingsHolder: ApplicationSettingsHolder,
                                       private val assetsHolder: AssetsHolder,
                                       private val balancesService: BalancesService) {
    fun create(logger: Logger?, validate: Boolean = true): WalletOperationsProcessor {

        return WalletOperationsProcessor(balancesService,
                currentTransactionBalancesHolderFactory.create(),
                applicationSettingsHolder,
                assetsHolder,
                validate,
                logger)
    }
}
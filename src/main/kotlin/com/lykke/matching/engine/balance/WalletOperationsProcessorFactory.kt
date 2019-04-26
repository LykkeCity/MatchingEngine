package com.lykke.matching.engine.balance

import com.lykke.matching.engine.holders.ApplicationSettingsHolder
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.order.transaction.CurrentTransactionBalancesHolderFactory
import org.slf4j.Logger
import org.springframework.stereotype.Component

@Component
class WalletOperationsProcessorFactory(private val currentTransactionBalancesHolderFactory: CurrentTransactionBalancesHolderFactory,
                                       private val applicationSettingsHolder: ApplicationSettingsHolder,
                                       private val assetsHolder: AssetsHolder) {
    fun create(logger: Logger?): WalletOperationsProcessor {

        return WalletOperationsProcessor(
                currentTransactionBalancesHolderFactory.create(),
                applicationSettingsHolder,
                assetsHolder,
                logger)
    }
}
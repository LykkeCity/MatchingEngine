package com.lykke.matching.engine.database.reconciliation

import com.lykke.matching.engine.common.SimpleApplicationEventPublisher
import com.lykke.matching.engine.database.reconciliation.events.AccountPersistEvent
import com.lykke.matching.engine.holders.BalancesDatabaseAccessorsHolder
import com.lykke.matching.engine.utils.config.Config
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.core.annotation.Order
import org.springframework.stereotype.Component

@Component
@Order(1)
class AccountsReconciliationService(private val persistedWalletsApplicationEventPublisher: SimpleApplicationEventPublisher<AccountPersistEvent>,
                                    private val balancesDatabaseAccessorsHolder: BalancesDatabaseAccessorsHolder) : ApplicationRunner {

    @Autowired
    private lateinit var config: Config

    override fun run(args: ApplicationArguments?) {
        if (balancesDatabaseAccessorsHolder.secondaryAccessor != null && !config.me.walletsMigration) {
            persistedWalletsApplicationEventPublisher.publishEvent(AccountPersistEvent(balancesDatabaseAccessorsHolder.primaryAccessor.loadWallets().values.toList()))
        }
    }
}
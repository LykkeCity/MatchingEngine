package com.lykke.matching.engine

import com.lykke.matching.engine.notification.BalanceUpdateNotification
import com.lykke.matching.engine.utils.migration.AccountsMigrationService
import com.lykke.matching.engine.utils.migration.AccountsMigrationException
import com.lykke.utils.alivestatus.exception.CheckAppInstanceRunningException
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingQueue

@Component
class Application {
    @Autowired
    lateinit var socketServer: Runnable

    @Autowired
    lateinit var azureStatusProcessor: Runnable

    @Autowired
    lateinit var accountsMigrationService: AccountsMigrationService

    @Autowired
    lateinit var balanceUpdateNotificationQueue: BlockingQueue<BalanceUpdateNotification>

    @Autowired
    @Qualifier("appStarterLogger")
    lateinit var LOGGER: Logger

    fun run () {
        try {
            azureStatusProcessor.run()
        } catch (e: CheckAppInstanceRunningException) {
            LOGGER.error("Error occurred while starting application, ${e.message}", e)
            System.exit(1)
        }

        try {
            accountsMigrationService.migrateAccountsIfConfigured()
        } catch (e: AccountsMigrationException) {
            LOGGER.error("Error occurred while migrating accounts, ${e.message}", e)
            System.exit(1)
        }

        socketServer.run()
    }
}
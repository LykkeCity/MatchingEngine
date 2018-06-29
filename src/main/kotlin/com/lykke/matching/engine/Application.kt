package com.lykke.matching.engine

import com.lykke.matching.engine.notification.BalanceUpdateNotification
import com.lykke.matching.engine.utils.balance.correctReservedVolumesIfNeed
import com.lykke.matching.engine.utils.config.Config
import com.lykke.matching.engine.utils.migration.AccountsMigrationException
import com.lykke.matching.engine.utils.migration.migrateAccountsIfConfigured
import com.lykke.utils.AppInitializer
import com.lykke.utils.alivestatus.exception.CheckAppInstanceRunningException
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.ApplicationContext
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingQueue

@Component
class Application {
    @Autowired
    private lateinit var config: Config

    @Autowired
    lateinit var socketServer: Runnable

    @Autowired
    lateinit var azureStatusProcessor: Runnable

    @Autowired
    lateinit var applicationContext: ApplicationContext

    @Autowired
    lateinit var balanceUpdateNotificationQueue: BlockingQueue<BalanceUpdateNotification>

    fun run () {
        try {
            azureStatusProcessor.run()
        } catch (e: CheckAppInstanceRunningException) {
            LOGGER.error(e.message)
            System.exit(1)
        }

        try {
            migrateAccountsIfConfigured(applicationContext)
        } catch (e: AccountsMigrationException) {
            AppInitializer.teeLog(e.message)
            System.exit(1)
        }

        correctReservedVolumesIfNeed(config, applicationContext, balanceUpdateNotificationQueue)
        socketServer.run()
    }
}
package com.lykke.matching.engine

import com.lykke.matching.engine.utils.balance.correctReservedVolumesIfNeed
import com.lykke.matching.engine.utils.config.Config
import com.lykke.matching.engine.utils.migration.AccountsMigrationService
import com.lykke.matching.engine.utils.migration.AccountsMigrationException
import com.lykke.matching.engine.utils.migration.OrdersMigrationService
import com.lykke.utils.AppInitializer
import com.lykke.utils.alivestatus.exception.CheckAppInstanceRunningException
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.ApplicationContext
import org.springframework.context.ApplicationEventPublisher
import org.springframework.stereotype.Component

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
    lateinit var applicationEventPublisher: ApplicationEventPublisher

    fun run () {
        try {
            azureStatusProcessor.run()
        } catch (e: CheckAppInstanceRunningException) {
            LOGGER.error(e.message)
            System.exit(1)
        }

        try {
            applicationContext.getBean(AccountsMigrationService::class.java).migrateAccountsIfConfigured()
        } catch (e: AccountsMigrationException) {
            AppInitializer.teeLog(e.message)
            System.exit(1)
        }

        try {
            applicationContext.getBean(OrdersMigrationService::class.java).migrateOrdersIfConfigured()
        } catch (e: Exception) {
            AppInitializer.teeLog("Unable to migrate orders: ${e.message}")
            System.exit(1)
        }

        correctReservedVolumesIfNeed(config, applicationContext, applicationEventPublisher)
        socketServer.run()
    }
}
package com.lykke.matching.engine

import com.lykke.matching.engine.utils.balance.ReservedVolumesRecalculator
import com.lykke.matching.engine.utils.migration.AccountsMigrationService
import com.lykke.matching.engine.utils.migration.AccountsMigrationException
import com.lykke.matching.engine.utils.migration.OrdersMigrationService
import com.lykke.utils.AppInitializer
import com.lykke.utils.alivestatus.exception.CheckAppInstanceRunningException
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.ApplicationContext
import org.springframework.stereotype.Component

@Component
class Application {
    @Autowired
    lateinit var socketServer: Runnable

    @Autowired
    lateinit var azureStatusProcessor: Runnable


    @Autowired
    lateinit var applicationContext: ApplicationContext

    @Autowired
    lateinit var reservedVolumesRecalculator: ReservedVolumesRecalculator

    fun run () {
        try {
            azureStatusProcessor.run()
        } catch (e: CheckAppInstanceRunningException) {
            AppInitializer.teeLog("Error occurred while starting application ${e.message}")
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

        reservedVolumesRecalculator.correctReservedVolumesIfNeed()
        socketServer.run()
    }
}
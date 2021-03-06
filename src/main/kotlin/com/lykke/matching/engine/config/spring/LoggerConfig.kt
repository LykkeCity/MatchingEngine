package com.lykke.matching.engine.config.spring

import com.lykke.matching.engine.database.azure.AzureMessageLogDatabaseAccessor
import com.lykke.matching.engine.holders.UUIDHolder
import com.lykke.matching.engine.logging.DatabaseLogger
import com.lykke.matching.engine.logging.MessageWrapper
import com.lykke.matching.engine.outgoing.messages.BalanceUpdate
import com.lykke.matching.engine.outgoing.messages.CashOperation
import com.lykke.matching.engine.outgoing.messages.CashTransferOperation
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.outgoing.messages.MarketOrderWithTrades
import com.lykke.matching.engine.outgoing.messages.ReservedCashOperation
import com.lykke.matching.engine.utils.config.Config
import com.lykke.utils.AppInitializer
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingQueue
import javax.annotation.PostConstruct

@Component
open class LoggerConfig {

    @Autowired
    private lateinit var config: Config

    @Bean(destroyMethod = "")
    open fun appStarterLogger(): Logger {
        return LoggerFactory.getLogger("AppStarter")
    }

    @Bean
    open fun singleLimitOrderPreProcessingLogger(): ThrottlingLogger {
        return ThrottlingLogger.getLogger("SingleLimitOrderPreProcessing")
    }

    @Bean
    open fun cashInOutPreProcessingLogger(): ThrottlingLogger {
        return ThrottlingLogger.getLogger("CashInOutPreProcessing")
    }

    @Bean
    open fun cashTransferPreProcessingLogger(): ThrottlingLogger {
        return ThrottlingLogger.getLogger("CashTransferPreProcessing")
    }

    @Bean
    open fun limitOrderCancelPreProcessingLogger(): ThrottlingLogger {
        return ThrottlingLogger.getLogger("LimitOrderCancelPreProcessing")
    }

    @Bean
    open fun limitOrderMassCancelPreProcessingLogger(): ThrottlingLogger {
        return ThrottlingLogger.getLogger("LimitOrderMassCancelPreProcessing")
    }

    @Bean
    open fun balanceUpdatesDatabaseLogger(@Value("\${azure.logs.blob.container}")
                                          logBlobName: String,
                                          @Value("\${azure.logs.balance.update.table}")
                                          logTable: String,
                                          uuidHolder: UUIDHolder,
                                          balanceUpdatesLogQueue: BlockingQueue<MessageWrapper>): DatabaseLogger<*> {
        return DatabaseLogger<BalanceUpdate>(AzureMessageLogDatabaseAccessor(config.me.db.messageLogConnString,
                logTable, logBlobName, uuidHolder), balanceUpdatesLogQueue)
    }

    @Bean
    open fun cashInOutDatabaseLogger(@Value("\${azure.logs.blob.container}")
                                     logBlobName: String,
                                     @Value("\${azure.logs.cash.operations.table}")
                                     logTable: String,
                                     uuidHolder: UUIDHolder,
                                     cashInOutLogQueue: BlockingQueue<MessageWrapper>): DatabaseLogger<*> {
        return DatabaseLogger<CashOperation>(AzureMessageLogDatabaseAccessor(config.me.db.messageLogConnString,
                logTable, logBlobName, uuidHolder), cashInOutLogQueue)
    }

    @Bean
    open fun cashTransferDatabaseLogger(@Value("\${azure.logs.blob.container}")
                                        logBlobName: String,
                                        @Value("\${azure.logs.transfers.events.table}")
                                        logTable: String,
                                        uuidHolder: UUIDHolder,
                                        cashTransferLogQueue: BlockingQueue<MessageWrapper>): DatabaseLogger<*> {
        return DatabaseLogger<CashTransferOperation>(AzureMessageLogDatabaseAccessor(config.me.db.messageLogConnString,
                logTable, logBlobName, uuidHolder), cashTransferLogQueue)
    }

    @Bean
    open fun clientLimitOrderDatabaseLogger(@Value("\${azure.logs.blob.container}")
                                            logBlobName: String,
                                            @Value("\${azure.logs.limit.orders.table}")
                                            logTable: String,
                                            uuidHolder: UUIDHolder,
                                            clientLimitOrdersLogQueue: BlockingQueue<MessageWrapper>): DatabaseLogger<*> {
        return DatabaseLogger<LimitOrdersReport>(AzureMessageLogDatabaseAccessor(config.me.db.messageLogConnString,
                logTable, logBlobName, uuidHolder), clientLimitOrdersLogQueue)
    }

    @Bean
    open fun marketOrderWithTradesDatabaseLogger(@Value("\${azure.logs.blob.container}")
                                                 logBlobName: String,
                                                 @Value("\${azure.logs.market.orders.table}")
                                                 logTable: String,
                                                 uuidHolder: UUIDHolder,
                                                 marketOrderWithTradesLogQueue: BlockingQueue<MessageWrapper>): DatabaseLogger<*> {
        return DatabaseLogger<MarketOrderWithTrades>(AzureMessageLogDatabaseAccessor(config.me.db.messageLogConnString,
                logTable, logBlobName, uuidHolder), marketOrderWithTradesLogQueue)
    }

    @Bean
    open fun reservedCashOperationDatabaseLogger(@Value("\${azure.logs.blob.container}")
                                                 logBlobName: String,
                                                 @Value("\${azure.logs.reserved.cash.operations.table}")
                                                 logTable: String,
                                                 uuidHolder: UUIDHolder,
                                                 reservedCashOperationLogQueue: BlockingQueue<MessageWrapper>): DatabaseLogger<*> {
        return DatabaseLogger<ReservedCashOperation>(AzureMessageLogDatabaseAccessor(config.me.db.messageLogConnString,
                logTable, logBlobName, uuidHolder), reservedCashOperationLogQueue)
    }

    @PostConstruct
    open fun init() {
        AppInitializer.init()
        MetricsLogger.init("ME", config.slackNotifications)
        ThrottlingLogger.init(config.throttlingLogger)
    }
}
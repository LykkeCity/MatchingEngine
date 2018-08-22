package com.lykke.matching.engine.outgoing.rabbit.impl.listeners

import com.lykke.matching.engine.database.azure.AzureMessageLogDatabaseAccessor
import com.lykke.matching.engine.logging.DatabaseLogger
import com.lykke.matching.engine.outgoing.messages.ReservedCashOperation
import com.lykke.matching.engine.outgoing.rabbit.RabbitMqService
import com.lykke.matching.engine.utils.config.Config
import com.lykke.utils.AppVersion
import com.rabbitmq.client.BuiltinExchangeType
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingQueue
import javax.annotation.PostConstruct

@Component
class ReservedCashOperationListener {
    @Autowired
    private lateinit var reservedCashOperationQueue: BlockingQueue<ReservedCashOperation>

    @Autowired
    private lateinit var rabbitMqService: RabbitMqService<Any>

    @Autowired
    private lateinit var config: Config

    @Value("\${azure.logs.blob.container}")
    private lateinit var logBlobName: String

    @Value("\${azure.logs.reserved.cash.operations.table}")
    private lateinit var logTable: String

    @PostConstruct
    fun initRabbitMqPublisher() {
        rabbitMqService.startPublisher(config.me.rabbitMqConfigs.reservedCashOperations, reservedCashOperationQueue,
                config.me.name,
                AppVersion.VERSION,
                BuiltinExchangeType.FANOUT,
                DatabaseLogger(
                        AzureMessageLogDatabaseAccessor(config.me.db.messageLogConnString,
                                logTable, logBlobName)))
    }
}
package com.lykke.matching.engine.messages

import com.lykke.matching.engine.database.LimitOrderDatabaseAccessor
import com.lykke.matching.engine.database.WalletDatabaseAccessor
import com.lykke.matching.engine.database.azure.AzureLimitOrderDatabaseAccessor
import com.lykke.matching.engine.database.azure.AzureWalletDatabaseAccessor
import com.lykke.matching.engine.services.CashOperationService
import com.lykke.matching.engine.services.LimitOrderService
import org.apache.log4j.Logger
import java.util.Properties
import java.util.concurrent.BlockingQueue

class MessageProcessor: Thread {

    companion object {
        val LOGGER = Logger.getLogger(MessageProcessor::class.java.name)
    }

    val messagesQueue: BlockingQueue<MessageWrapper>

    val walletDatabaseAccessor: WalletDatabaseAccessor
    val limitOrderDatabaseAccessor: LimitOrderDatabaseAccessor

    val cashOperationService: CashOperationService
    val limitOrderService: LimitOrderService

    constructor(config: Properties, queue: BlockingQueue<MessageWrapper>) {
        this.messagesQueue = queue
        this.walletDatabaseAccessor = AzureWalletDatabaseAccessor(config)
        this.limitOrderDatabaseAccessor = AzureLimitOrderDatabaseAccessor(config)

        this.cashOperationService = CashOperationService(this.walletDatabaseAccessor)
        this.limitOrderService = LimitOrderService(this.limitOrderDatabaseAccessor)
    }

    override fun run() {
        while (true) {
            processMessage(messagesQueue.take())
        }
    }

    private fun processMessage(message: MessageWrapper) {
        when (message.type) {
            //MessageType.PING -> already processed by client handler
            MessageType.UPDATE_BALANCE -> {
                cashOperationService.processMessage(message.byteArray)
            }
            MessageType.LIMIT_ORDER -> {
                limitOrderService.processMessage(message.byteArray)
            }
            else -> {
                LOGGER.error("Unknown message type: ${message.type}")
            }
        }
    }
}
package com.lykke.matching.engine.services

import com.lykke.matching.engine.balance.BalanceException
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.database.WalletDatabaseAccessor
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.services.validators.impl.CashOperationValidator
import com.lykke.matching.engine.services.validators.impl.ValidationException
import com.lykke.matching.engine.utils.NumberUtils
import com.lykke.utils.logging.MetricsLogger
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import java.util.Date
import java.util.UUID

@Service
class CashOperationService @Autowired constructor (private val balancesHolder: BalancesHolder,
                                                   private val cashOperationValidator: CashOperationValidator): AbstractService {
    companion object {
        val LOGGER = Logger.getLogger(CashOperationService::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    override fun processMessage(messageWrapper: MessageWrapper) {
        val message = getMessage(messageWrapper)
        LOGGER.debug("Processing cash operation (${message.bussinesId}) for client ${message.clientId}, " +
                "asset ${message.assetId}, amount: ${NumberUtils.roundForPrint(message.amount)}")

        try {
            cashOperationValidator.performValidation(message)
        } catch (e: ValidationException) {
            LOGGER.info(e.message)
            writeErrorResponse(messageWrapper)
            return
        }

        val operation = WalletOperation(UUID.randomUUID().toString(), message.uid.toString(), message.clientId, message.assetId,
                Date(message.timestamp), message.amount, 0.0)

        try {
            balancesHolder.createWalletProcessor(LOGGER).preProcess(listOf(operation)).apply(message.uid.toString(), MessageType.CASH_OPERATION.name)
        } catch (e: BalanceException) {
            LOGGER.info("Unable to process cash operation (${message.bussinesId}): ${e.message}")
            messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder().setUid(message.uid).setBussinesId(message.bussinesId).build())
            return
        }

        messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder().setUid(message.uid).setBussinesId(message.bussinesId).setRecordId(operation.id).build())
        LOGGER.debug("Cash operation (${message.bussinesId}) for client ${message.clientId}, asset ${message.assetId}, amount: ${NumberUtils.roundForPrint(message.amount)} processed")
    }

    private fun parse(array: ByteArray): ProtocolMessages.CashOperation {
        return ProtocolMessages.CashOperation.parseFrom(array)
    }

    override fun parseMessage(messageWrapper: MessageWrapper) {
        val message = parse(messageWrapper.byteArray)
        messageWrapper.messageId = message.bussinesId
        messageWrapper.timestamp = message.timestamp
        messageWrapper.parsedMessage = message
    }

    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus) {
        val message = getMessage(messageWrapper)
        messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder().setUid(message.uid).setBussinesId(message.bussinesId).build())
    }

    fun writeErrorResponse(messageWrapper: MessageWrapper) {
        val message = getMessage(messageWrapper)
        messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder()
                .setUid(message.uid)
                .setBussinesId(message.bussinesId).build())
    }

    private fun getMessage(messageWrapper: MessageWrapper) =
            messageWrapper.parsedMessage!! as ProtocolMessages.CashOperation
}
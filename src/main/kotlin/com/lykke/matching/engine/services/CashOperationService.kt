package com.lykke.matching.engine.services

import com.lykke.matching.engine.balance.BalanceException
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.services.validators.CashOperationValidator
import com.lykke.matching.engine.services.validators.impl.ValidationException
import com.lykke.matching.engine.utils.NumberUtils
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import java.math.BigDecimal
import java.util.Date
import java.util.UUID

@Service
class CashOperationService @Autowired constructor (private val balancesHolder: BalancesHolder,
                                                   private val cashOperationValidator: CashOperationValidator): AbstractService {
    companion object {
        private val LOGGER = Logger.getLogger(CashOperationService::class.java.name)
    }

    override fun processMessage(messageWrapper: MessageWrapper) {
        val message = getMessage(messageWrapper)
        LOGGER.debug("Processing cash messageId: ${messageWrapper.messageId}," +
                " operation (${message.bussinesId}),for client ${message.clientId}, " +
                "asset ${message.assetId}, amount: ${NumberUtils.roundForPrint(message.amount)}")

        try {
            cashOperationValidator.performValidation(message)
        } catch (e: ValidationException) {
            writeErrorResponse(messageWrapper)
            return
        }

        val operation = WalletOperation(UUID.randomUUID().toString(), message.uid.toString(), message.clientId, message.assetId,
                Date(message.timestamp), BigDecimal.valueOf(message.amount), BigDecimal.ZERO)

        val walletProcessor = balancesHolder.createWalletProcessor(LOGGER)
        try {
            walletProcessor.preProcess(listOf(operation))
        } catch (e: BalanceException) {
            LOGGER.info("Unable to process cash operation (${message.bussinesId}): ${e.message}")
            messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder()
                    .setBussinesId(message.bussinesId))
            return
        }

        val updated = walletProcessor.persistBalances(ProcessedMessage(messageWrapper.type, messageWrapper.timestamp!!, messageWrapper.messageId!!), null, null)
        messageWrapper.processedMessagePersisted = true
        if (updated) {
            walletProcessor.apply().sendNotification(message.uid.toString(), MessageType.CASH_OPERATION.name, messageWrapper.messageId!!)
        }

        messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder()
                .setBussinesId(message.bussinesId)
                .setRecordId(operation.id))
        LOGGER.debug("Cash operation (${message.bussinesId}) for client ${message.clientId}, asset ${message.assetId}, amount: ${NumberUtils.roundForPrint(message.amount)} processed")
    }

    private fun parse(array: ByteArray): ProtocolMessages.CashOperation {
        return ProtocolMessages.CashOperation.parseFrom(array)
    }

    override fun parseMessage(messageWrapper: MessageWrapper) {
        val message = parse(messageWrapper.byteArray)
        messageWrapper.messageId = if (message.hasMessageId()) message.messageId else  message.bussinesId
        messageWrapper.timestamp = message.timestamp
        messageWrapper.parsedMessage = message
        messageWrapper.id = message.uid.toString()
    }

    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus) {
        val message = getMessage(messageWrapper)
        messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder()
                .setBussinesId(message.bussinesId))
    }

    fun writeErrorResponse(messageWrapper: MessageWrapper) {
        val message = getMessage(messageWrapper)
        messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder()
                .setBussinesId(message.bussinesId))
    }

    private fun getMessage(messageWrapper: MessageWrapper): ProtocolMessages.CashOperation {
        if (messageWrapper.parsedMessage == null) {
            parseMessage(messageWrapper)
        }
        return messageWrapper.parsedMessage!! as ProtocolMessages.CashOperation
    }

}
package com.lykke.matching.engine.services

import com.lykke.matching.engine.balance.BalanceException
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.database.WalletDatabaseAccessor
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.utils.NumberUtils
import com.lykke.utils.logging.MetricsLogger
import org.apache.log4j.Logger
import java.util.Date
import java.util.UUID

class CashOperationService(private val walletDatabaseAccessor: WalletDatabaseAccessor,
                           private val balancesHolder: BalancesHolder,
                           private val applicationSettingsCache: ApplicationSettingsCache): AbstractService {
    companion object {
        val LOGGER = Logger.getLogger(CashOperationService::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    override fun processMessage(messageWrapper: MessageWrapper) {
        val message = messageWrapper.parsedMessage!! as ProtocolMessages.CashOperation
        LOGGER.debug("Processing cash operation (${message.bussinesId}) for client ${message.clientId}, asset ${message.assetId}, amount: ${NumberUtils.roundForPrint(message.amount)}")

        if(!performValidation(message, messageWrapper)) {
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

    private fun performValidation(message: ProtocolMessages.CashOperation, messageWrapper: MessageWrapper): Boolean {
        val validationError = getFirstValidationError(message)

        when (validationError) {
            ValidationErrors.DEFAULT ->  { messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder()
                    .setUid(message.uid)
                    .setBussinesId(message.bussinesId)
                    .build())
                return false
            }
            ValidationErrors.PRICE_ACCURACY -> {
                messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder()
                        .setUid(message.uid)
                        .setBussinesId(message.bussinesId)
                        .build())
                return false
            }
        }

        return true
    }

    private fun getFirstValidationError(message: ProtocolMessages.CashOperation): ValidationErrors? {

        if (message.amount < 0 && applicationSettingsCache.isAssetDisabled(message.assetId)) {
            LOGGER.info("""Cash out operation (${message.uid})
                |for client ${message.clientId} asset ${message.assetId},
                |volume: ${NumberUtils.roundForPrint(message.amount)}: disabled asset""".trimMargin())
            return ValidationErrors.DEFAULT
        }

        if (message.amount < 0) {
            val balance = balancesHolder.getBalance(message.clientId, message.assetId)
            val reservedBalance = balancesHolder.getReservedBalance(message.clientId, message.assetId)
            if (balance - reservedBalance < Math.abs(message.amount)) {
                LOGGER.info("""Cash out operation (${message.uid})
                    for client ${message.clientId} asset ${message.assetId},
                    volume: ${NumberUtils.roundForPrint(message.amount)}:
                    low balance $balance, reserved balance $reservedBalance""")
                return ValidationErrors.DEFAULT
            }
        }

        return null
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
        val message = messageWrapper.parsedMessage!! as ProtocolMessages.CashOperation
        messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder().setUid(message.uid).setBussinesId(message.bussinesId).build())
    }

    private enum class ValidationErrors {
        DEFAULT, PRICE_ACCURACY
    }
}
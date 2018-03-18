package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.database.WalletDatabaseAccessor
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.utils.RoundingUtils
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

    private var messagesCount: Long = 0

    override fun processMessage(messageWrapper: MessageWrapper) {
        val message = messageWrapper.parsedMessage!! as ProtocolMessages.CashOperation
        LOGGER.debug("Processing cash operation (${message.bussinesId}) for client ${message.clientId}, asset ${message.assetId}, amount: ${RoundingUtils.roundForPrint(message.amount)}")

        if (message.amount < 0 && applicationSettingsCache.isAssetDisabled(message.assetId)) {
            messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder().setUid(message.uid).setBussinesId(message.bussinesId).build())
            LOGGER.info("Cash out operation (${message.uid}) for client ${message.clientId} asset ${message.assetId}, volume: ${RoundingUtils.roundForPrint(message.amount)}: disabled asset")
            return
        }

        if (message.amount < 0) {
            val balance = balancesHolder.getBalance(message.clientId, message.assetId)
            val reservedBalance = balancesHolder.getReservedBalance(message.clientId, message.assetId)
            if (balance - reservedBalance < Math.abs(message.amount)) {
                messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder().setUid(message.uid).setBussinesId(message.bussinesId).build())
                LOGGER.info("Cash out operation (${message.uid}) for client ${message.clientId} asset ${message.assetId}, volume: ${RoundingUtils.roundForPrint(message.amount)}: low balance $balance, reserved balance $reservedBalance")
                return
            }
        }

        val operation = WalletOperation(UUID.randomUUID().toString(), message.uid.toString(), message.clientId, message.assetId,
                Date(message.timestamp), message.amount, 0.0)
        balancesHolder.processWalletOperations(message.uid.toString(), MessageType.CASH_OPERATION.name, listOf(operation))

        messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder().setUid(message.uid).setBussinesId(message.bussinesId).setRecordId(operation.id).build())
        LOGGER.debug("Cash operation (${message.bussinesId}) for client ${message.clientId}, asset ${message.assetId}, amount: ${RoundingUtils.roundForPrint(message.amount)} processed")
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
}
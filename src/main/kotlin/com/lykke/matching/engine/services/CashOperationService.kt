package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.ExternalCashOperation
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.database.WalletDatabaseAccessor
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.utils.RoundingUtils
import com.lykke.utils.logging.MetricsLogger
import org.apache.log4j.Logger
import java.util.Date
import java.util.UUID

class CashOperationService(private val walletDatabaseAccessor: WalletDatabaseAccessor,
                           private val balancesHolder: BalancesHolder): AbstractService<ProtocolMessages.CashOperation> {

    companion object {
        val LOGGER = Logger.getLogger(CashOperationService::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    private var messagesCount: Long = 0

    override fun processMessage(messageWrapper: MessageWrapper) {
        val message = parse(messageWrapper.byteArray)
        LOGGER.debug("Processing cash operation (${message.bussinesId}) for client ${message.clientId}, asset ${message.assetId}, amount: ${RoundingUtils.roundForPrint(message.amount)}, sendToBitcoin: ${message.sendToBitcoin}")

        val externalCashOperation = walletDatabaseAccessor.loadExternalCashOperation(message.clientId, message.bussinesId)
        if (externalCashOperation != null) {
            messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder().setUid(message.uid).setBussinesId(message.bussinesId)
                    .setRecordId(externalCashOperation.cashOperationId).build())
            LOGGER.debug("Cash operation (${message.bussinesId}) for client ${message.clientId}, asset ${message.assetId}, amount: ${RoundingUtils.roundForPrint(message.amount)}, sendToBitcoin: ${message.sendToBitcoin} already processed")
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
        walletDatabaseAccessor.insertExternalCashOperation(ExternalCashOperation(operation.clientId, message.bussinesId, operation.id))

        messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder().setUid(message.uid).setBussinesId(message.bussinesId)
                .setRecordId(operation.id).build())
        LOGGER.debug("Cash operation (${message.bussinesId}) for client ${message.clientId}, asset ${message.assetId}, amount: ${RoundingUtils.roundForPrint(message.amount)}, sendToBitcoin: ${message.sendToBitcoin} processed")
    }

    private fun parse(array: ByteArray): ProtocolMessages.CashOperation {
        return ProtocolMessages.CashOperation.parseFrom(array)
    }
}
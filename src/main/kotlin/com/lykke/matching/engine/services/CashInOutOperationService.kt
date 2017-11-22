package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.ExternalCashOperation
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.database.CashOperationsDatabaseAccessor
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.logging.MetricsLogger
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageStatus.ALREADY_PROCESSED
import com.lykke.matching.engine.messages.MessageStatus.OK
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.outgoing.messages.CashOperation
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.round
import com.lykke.matching.engine.utils.RoundingUtils
import org.apache.log4j.Logger
import java.util.Date
import java.util.UUID
import java.util.concurrent.BlockingQueue

class CashInOutOperationService(private val cashOperationsDatabaseAccessor: CashOperationsDatabaseAccessor,
                           private val assetsHolder: AssetsHolder,
                           private val balancesHolder: BalancesHolder,
                           private val rabbitCashInOutQueue: BlockingQueue<JsonSerializable>): AbstractService<ProtocolMessages.CashOperation> {

    companion object {
        val LOGGER = Logger.getLogger(CashInOutOperationService::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    override fun processMessage(messageWrapper: MessageWrapper) {
        val message = parse(messageWrapper.byteArray)
        LOGGER.debug("Processing cash in/out operation (${message.id}) for client ${message.clientId}, asset ${message.assetId}, amount: ${RoundingUtils.roundForPrint(message.volume)}")

        val externalCashOperation = cashOperationsDatabaseAccessor.loadExternalCashOperation(message.clientId, message.id)
        if (externalCashOperation != null) {
            messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setId(message.id).setMatchingEngineId(externalCashOperation.cashOperationId)
                    .setStatus(ALREADY_PROCESSED.type).setStatusReason("ID:${externalCashOperation.cashOperationId}").build())
            LOGGER.info("Cash in/out operation (${message.id}) for client ${message.clientId}, asset ${message.assetId}, amount: ${RoundingUtils.roundForPrint(message.volume)} already processed")
            return
        }

        val operation = WalletOperation(UUID.randomUUID().toString(), message.id, message.clientId, message.assetId,
                Date(message.timestamp), message.volume, 0.0)

        if (message.volume < 0) {
            val balance = balancesHolder.getBalance(message.clientId, message.assetId)
            val reservedBalance = balancesHolder.getReservedBalance(message.clientId, message.assetId)
            if (RoundingUtils.parseDouble(balance - reservedBalance + message.volume, assetsHolder.getAsset(operation.assetId).accuracy).toDouble() < 0.0) {
                messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setId(message.id).setMatchingEngineId(operation.id)
                        .setStatus(MessageStatus.LOW_BALANCE.type).build())
                LOGGER.info("Cash out operation (${message.id}) for client ${message.clientId} asset ${message.assetId}, volume: ${RoundingUtils.roundForPrint(message.volume)}: low balance $balance, reserved balance $reservedBalance")
                return
            }
        }

        balancesHolder.processWalletOperations(message.id, MessageType.CASH_IN_OUT_OPERATION.name, listOf(operation))
        cashOperationsDatabaseAccessor.insertExternalCashOperation(ExternalCashOperation(operation.clientId, message.id, operation.id))
        rabbitCashInOutQueue.put(CashOperation(message.id, operation.clientId, operation.dateTime, operation.amount.round(assetsHolder.getAsset(operation.assetId).accuracy), operation.assetId))

        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setId(message.id).setMatchingEngineId(operation.id).setStatus(OK.type).build())
        LOGGER.info("Cash in/out operation (${message.id}) for client ${message.clientId}, asset ${message.assetId}, amount: ${RoundingUtils.roundForPrint(message.volume)} processed")
    }

    private fun parse(array: ByteArray): ProtocolMessages.CashInOutOperation {
        return ProtocolMessages.CashInOutOperation.parseFrom(array)
    }
}
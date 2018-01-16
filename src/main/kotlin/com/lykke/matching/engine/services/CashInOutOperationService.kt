package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.daos.fee.NewFeeInstruction
import com.lykke.matching.engine.database.CashOperationsDatabaseAccessor
import com.lykke.matching.engine.fee.FeeException
import com.lykke.matching.engine.fee.FeeProcessor
import com.lykke.matching.engine.fee.checkFee
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageStatus.OK
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.outgoing.messages.CashOperation
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.round
import com.lykke.matching.engine.utils.RoundingUtils
import com.lykke.utils.logging.MetricsLogger
import org.apache.log4j.Logger
import java.util.Date
import java.util.UUID
import java.util.concurrent.BlockingQueue

class CashInOutOperationService(private val cashOperationsDatabaseAccessor: CashOperationsDatabaseAccessor,
                                private val assetsHolder: AssetsHolder,
                                private val balancesHolder: BalancesHolder,
                                private val rabbitCashInOutQueue: BlockingQueue<JsonSerializable>,
                                private val feeProcessor: FeeProcessor) : AbstractService {

    companion object {
        val LOGGER = Logger.getLogger(CashInOutOperationService::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    override fun processMessage(messageWrapper: MessageWrapper) {
        if (messageWrapper.parsedMessage == null) {
            parseMessage(messageWrapper)
        }
        val message = messageWrapper.parsedMessage!! as ProtocolMessages.CashInOutOperation
        val fees = NewFeeInstruction.create(message.feesList)
        LOGGER.debug("Processing cash in/out operation (${message.id}) for client ${message.clientId}, asset ${message.assetId}, amount: ${RoundingUtils.roundForPrint(message.volume)}, fees: $fees")

        val operationId = UUID.randomUUID().toString()
        if (!checkFee(null, fees)) {
            writeInvalidFeeResponse(messageWrapper, message, operationId)
            return
        }

        val operation = WalletOperation(operationId, message.id, message.clientId, message.assetId,
                Date(message.timestamp), message.volume, 0.0)
        val operations = mutableListOf(operation)

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

        val feeTransfers = try {
            feeProcessor.processFee(fees, operation, operations)
        } catch (e: FeeException) {
            writeInvalidFeeResponse(messageWrapper, message, operationId, e.message)
            return
        }

        balancesHolder.processWalletOperations(message.id, MessageType.CASH_IN_OUT_OPERATION.name, operations)
        rabbitCashInOutQueue.put(CashOperation(message.id, operation.clientId, operation.dateTime, operation.amount.round(assetsHolder.getAsset(operation.assetId).accuracy), operation.assetId, fees, feeTransfers))

        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setId(message.id).setMatchingEngineId(operation.id).setStatus(OK.type).build())
        LOGGER.info("Cash in/out operation (${message.id}) for client ${message.clientId}, asset ${message.assetId}, amount: ${RoundingUtils.roundForPrint(message.volume)} processed")
    }

    private fun parse(array: ByteArray): ProtocolMessages.CashInOutOperation {
        return ProtocolMessages.CashInOutOperation.parseFrom(array)
    }

    override fun parseMessage(messageWrapper: MessageWrapper) {
        val message = parse(messageWrapper.byteArray)
        messageWrapper.messageId = message.id
        messageWrapper.timestamp = message.timestamp
        messageWrapper.parsedMessage = message
    }

    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus) {
        val message = messageWrapper.parsedMessage!! as ProtocolMessages.CashInOutOperation
        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setId(message.id).setStatus(status.type).build())
    }

    private fun writeInvalidFeeResponse(messageWrapper: MessageWrapper, message: ProtocolMessages.CashInOutOperation, operationId: String, errorMessage: String = "invalid fee for client") {
        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                .setId(message.id)
                .setMatchingEngineId(operationId)
                .setStatus(MessageStatus.INVALID_FEE.type)
                .setStatusReason(errorMessage)
                .build())
        LOGGER.info("Cash in/out operation (${message.id}) for client ${message.clientId}, asset ${message.assetId}, amount: ${RoundingUtils.roundForPrint(message.volume)}: $errorMessage")
        return
    }
}
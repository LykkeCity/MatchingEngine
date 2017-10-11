package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.ExternalCashOperation
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.database.WalletDatabaseAccessor
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.logging.ASSET
import com.lykke.matching.engine.logging.CLIENT_ID
import com.lykke.matching.engine.logging.KeyValue
import com.lykke.matching.engine.logging.Line
import com.lykke.matching.engine.logging.ME_RESERVED_CASH_OPERATION
import com.lykke.matching.engine.logging.MetricsLogger
import com.lykke.matching.engine.logging.RESERVED_AMOUNT
import com.lykke.matching.engine.logging.TIMESTAMP
import com.lykke.matching.engine.logging.UID
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.outgoing.messages.ReservedCashOperation
import com.lykke.matching.engine.round
import com.lykke.matching.engine.utils.RoundingUtils
import org.apache.log4j.Logger
import java.time.LocalDateTime
import java.util.Date
import java.util.UUID
import java.util.concurrent.BlockingQueue

class ReservedCashInOutOperationService(
        private val walletDatabaseAccessor: WalletDatabaseAccessor,
        private val assetsHolder: AssetsHolder,
        private val balancesHolder: BalancesHolder,
        private val rabbitCashInOutQueue: BlockingQueue<JsonSerializable>
) : AbstractService {

    companion object {
        private val LOGGER = Logger.getLogger(ReservedCashInOutOperationService::class.java.name)
        private val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    private var messagesCount: Long = 0

    override fun processMessage(messageWrapper: MessageWrapper) {
        val message = parse(messageWrapper.byteArray)
        LOGGER.debug("Processing reserved cash in/out operation (${message.id}) for client ${message.clientId}, asset ${message.assetId}, amount: ${RoundingUtils.roundForPrint(message.reservedVolume)}")

        val externalCashOperation = walletDatabaseAccessor.loadExternalCashOperation(message.clientId, message.id)
        if (externalCashOperation != null) {
            messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setId(message.id).setMatchingEngineId(externalCashOperation.cashOperationId)
                    .setStatus(MessageStatus.ALREADY_PROCESSED.type).setStatusReason("ID:${externalCashOperation.cashOperationId}").build())
            LOGGER.info("Reserved cash in/out operation (${message.id}) for client ${message.clientId}, asset ${message.assetId}, amount: ${RoundingUtils.roundForPrint(message.reservedVolume)} already processed")
            return
        }

        val operation = WalletOperation(UUID.randomUUID().toString(), message.id, message.clientId, message.assetId,
                Date(message.timestamp), 0.0, message.reservedVolume)

        val reservedBalance = balancesHolder.getReservedBalance(message.clientId, message.assetId)
        val accuracy = assetsHolder.getAsset(operation.assetId).accuracy
        if (message.reservedVolume < 0) {
            if (RoundingUtils.parseDouble(reservedBalance + message.reservedVolume, accuracy).toDouble() < 0.0) {
                messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setId(message.id).setMatchingEngineId(operation.id)
                        .setStatus(MessageStatus.LOW_BALANCE.type).build())
                LOGGER.info("Reserved cash out operation (${message.id}) for client ${message.clientId} asset ${message.assetId}, volume: ${RoundingUtils.roundForPrint(message.reservedVolume)}: low reserved balance $reservedBalance")
                return
            }
        } else {
            val balance = balancesHolder.getBalance(message.clientId, message.assetId)
            if (RoundingUtils.parseDouble(balance - reservedBalance - message.reservedVolume, accuracy).toDouble() < 0.0) {
                messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setId(message.id).setMatchingEngineId(operation.id)
                        .setStatus(MessageStatus.RESERVED_VOLUME_HIGHER_THAN_BALANCE.type).build())
                LOGGER.info("Reserved cash in operation (${message.id}) for client ${message.clientId} asset ${message.assetId}, volume: ${RoundingUtils.roundForPrint(message.reservedVolume)}: low balance $balance, current reserved balance $reservedBalance")
                return
            }
        }

        balancesHolder.processWalletOperations(message.id, MessageType.RESERVED_CASH_IN_OUT_OPERATION.name, listOf(operation))
        walletDatabaseAccessor.insertExternalCashOperation(ExternalCashOperation(operation.clientId, message.id, operation.id))
        rabbitCashInOutQueue.put(ReservedCashOperation(message.id, operation.clientId, operation.dateTime, operation.reservedAmount.round(accuracy), operation.assetId))

        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setId(message.id).setMatchingEngineId(operation.id).setStatus(MessageStatus.OK.type).build())
        LOGGER.info("Reserved cash in/out operation (${message.id}) for client ${message.clientId}, asset ${message.assetId}, amount: ${RoundingUtils.roundForPrint(message.reservedVolume)} processed")

        METRICS_LOGGER.log(Line(ME_RESERVED_CASH_OPERATION, arrayOf(
                KeyValue(UID, message.id),
                KeyValue(TIMESTAMP, LocalDateTime.now().format(MetricsLogger.DATE_TIME_FORMATTER)),
                KeyValue(CLIENT_ID, message.clientId),
                KeyValue(ASSET, message.assetId),
                KeyValue(RESERVED_AMOUNT, message.reservedVolume.toString())  // fixme: or com.lykke.matching.engine.logging.AMOUNT ?
        )))
        METRICS_LOGGER.log(KeyValue(ME_RESERVED_CASH_OPERATION, (++messagesCount).toString()))
    }

    private fun parse(array: ByteArray): ProtocolMessages.ReservedCashInOutOperation {
        return ProtocolMessages.ReservedCashInOutOperation.parseFrom(array)
    }

}
package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.ExternalCashOperation
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.database.WalletDatabaseAccessor
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.logging.AMOUNT
import com.lykke.matching.engine.logging.ASSET
import com.lykke.matching.engine.logging.CLIENT_ID
import com.lykke.matching.engine.logging.KeyValue
import com.lykke.matching.engine.logging.Line
import com.lykke.matching.engine.logging.ME_CASH_OPERATION
import com.lykke.matching.engine.logging.MetricsLogger
import com.lykke.matching.engine.logging.TIMESTAMP
import com.lykke.matching.engine.logging.UID
import com.lykke.matching.engine.messages.MessageStatus.ALREADY_PROCESSED
import com.lykke.matching.engine.messages.MessageStatus.OK
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.outgoing.messages.CashOperation
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.round
import com.lykke.matching.engine.utils.RoundingUtils
import org.apache.log4j.Logger
import java.time.LocalDateTime
import java.util.Date
import java.util.UUID
import java.util.concurrent.BlockingQueue

class CashInOutOperationService(private val walletDatabaseAccessor: WalletDatabaseAccessor,
                           private val assetsHolder: AssetsHolder,
                           private val balancesHolder: BalancesHolder,
                           private val rabbitCashInOutQueue: BlockingQueue<JsonSerializable>): AbsractService<ProtocolMessages.CashOperation> {

    companion object {
        val LOGGER = Logger.getLogger(CashInOutOperationService::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    private var messagesCount: Long = 0

    override fun processMessage(messageWrapper: MessageWrapper) {
        val message = parse(messageWrapper.byteArray)
        LOGGER.debug("Processing cash in/out operation (${message.id}) for client ${message.clientId}, asset ${message.assetId}, amount: ${RoundingUtils.roundForPrint(message.volume)}")

        val externalCashOperation = walletDatabaseAccessor.loadExternalCashOperation(message.clientId, message.id)
        if (externalCashOperation != null) {
            messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setId(message.id).setMatchingEngineId(externalCashOperation.cashOperationId)
                    .setStatus(ALREADY_PROCESSED.type).setStatusReason("ID:${externalCashOperation.cashOperationId}").build())
            LOGGER.info("Cash in/out operation (${message.id}) for client ${message.clientId}, asset ${message.assetId}, amount: ${RoundingUtils.roundForPrint(message.volume)} already processed")
            return
        }

        val operation = WalletOperation(UUID.randomUUID().toString(), message.id, message.clientId, message.assetId,
                Date(message.timestamp), message.volume)
        balancesHolder.processWalletOperations(listOf(operation))

        walletDatabaseAccessor.insertExternalCashOperation(ExternalCashOperation(operation.clientId, message.id, operation.id))

        rabbitCashInOutQueue.put(CashOperation(message.id, operation.clientId, operation.dateTime, operation.amount.round(assetsHolder.getAsset(operation.assetId).accuracy), operation.assetId))

        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setId(message.id).setMatchingEngineId(operation.id).setStatus(OK.type).build())
        LOGGER.info("Cash in/out operation (${message.id}) for client ${message.clientId}, asset ${message.assetId}, amount: ${RoundingUtils.roundForPrint(message.volume)} processed")

        METRICS_LOGGER.log(Line(ME_CASH_OPERATION, arrayOf(
                KeyValue(UID, message.id),
                KeyValue(TIMESTAMP, LocalDateTime.now().format(MetricsLogger.DATE_TIME_FORMATTER)),
                KeyValue(CLIENT_ID, message.clientId),
                KeyValue(ASSET, message.assetId),
                KeyValue(AMOUNT, message.volume.toString())
        )))
        METRICS_LOGGER.log(KeyValue(ME_CASH_OPERATION, (++messagesCount).toString()))
    }

    private fun parse(array: ByteArray): ProtocolMessages.CashInOutOperation {
        return ProtocolMessages.CashInOutOperation.parseFrom(array)
    }
}
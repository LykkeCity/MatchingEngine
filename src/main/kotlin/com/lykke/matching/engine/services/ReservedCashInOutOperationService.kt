package com.lykke.matching.engine.services

import com.lykke.matching.engine.balance.BalanceException
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.balance.WalletOperationsProcessorFactory
import com.lykke.matching.engine.database.PersistenceManager
import com.lykke.matching.engine.database.common.entity.PersistenceData
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.MessageProcessingStatusHolder
import com.lykke.matching.engine.holders.MessageSequenceNumberHolder
import com.lykke.matching.engine.holders.UUIDHolder
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.outgoing.messages.ReservedCashInOutEventData
import com.lykke.matching.engine.outgoing.senders.OutgoingEventProcessor
import com.lykke.matching.engine.services.validators.ReservedCashInOutOperationValidator
import com.lykke.matching.engine.services.validators.impl.ValidationException
import com.lykke.matching.engine.utils.NumberUtils
import com.lykke.matching.engine.utils.order.MessageStatusUtils
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import java.math.BigDecimal
import java.util.Date

@Service
class ReservedCashInOutOperationService @Autowired constructor (private val assetsHolder: AssetsHolder,
                                                                private val walletOperationsProcessorFactory: WalletOperationsProcessorFactory,
                                                                private val reservedCashInOutOperationValidator: ReservedCashInOutOperationValidator,
                                                                private val messageProcessingStatusHolder: MessageProcessingStatusHolder,
                                                                private val persistenceManager: PersistenceManager,
                                                                private val messageSequenceNumberHolder: MessageSequenceNumberHolder,
                                                                private val uuidHolder: UUIDHolder,
                                                                private val outgoingEventProcessor: OutgoingEventProcessor) : AbstractService {

    companion object {
        private val LOGGER = LoggerFactory.getLogger(ReservedCashInOutOperationService::class.java.name)
    }

    override fun processMessage(messageWrapper: MessageWrapper) {
        if (messageWrapper.parsedMessage == null) {
            parseMessage(messageWrapper)
        }
        val message = getMessage(messageWrapper)
        val asset = assetsHolder.getAsset(message.assetId)
        if ((isCashIn(message.reservedVolume) && messageProcessingStatusHolder.isCashInDisabled(asset)) ||
                (!isCashIn(message.reservedVolume) && messageProcessingStatusHolder.isCashOutDisabled(asset))) {
            writeResponse(messageWrapper, MessageStatus.MESSAGE_PROCESSING_DISABLED)
            return
        }

        LOGGER.debug("Processing reserved cash in/out messageId: ${messageWrapper.messageId} " +
                "operation (${message.id}) for client ${message.clientId}, " +
                "asset ${message.assetId}, amount: ${NumberUtils.roundForPrint(message.reservedVolume)}")

        val now = Date()
        val matchingEngineOperationId = uuidHolder.getNextValue()
        val operation = WalletOperation(message.clientId, message.assetId, BigDecimal.ZERO, BigDecimal.valueOf(message.reservedVolume))

        try {
            reservedCashInOutOperationValidator.performValidation(message)
        } catch (e: ValidationException) {
            writeErrorResponse(messageWrapper, matchingEngineOperationId, MessageStatusUtils.toMessageStatus(e.validationType), e.message)
            return
        }

        val accuracy = asset.accuracy

        val walletProcessor = walletOperationsProcessorFactory.create(LOGGER)
        try {
            walletProcessor.preProcess(listOf(operation), allowTrustedClientReservedBalanceOperation = true)
        } catch (e: BalanceException) {
            LOGGER.info("Reserved cash in/out operation (${message.id}) failed due to invalid balance: ${e.message}")
            writeErrorResponse(messageWrapper, matchingEngineOperationId, MessageStatus.LOW_BALANCE, e.message)
            return
        }

        val sequenceNumber = messageSequenceNumberHolder.getNewValue()
        val updated = persistenceManager.persist(PersistenceData(walletProcessor.persistenceData(),
                messageWrapper.processedMessage,
                null,
                null,
                sequenceNumber))
        messageWrapper.triedToPersist = true
        messageWrapper.persisted = updated
        if (!updated) {
            writeErrorResponse(messageWrapper, matchingEngineOperationId, MessageStatus.RUNTIME)
            LOGGER.info("Reserved cash in/out operation (${message.id}) for client ${message.clientId} asset ${message.assetId}, volume: ${NumberUtils.roundForPrint(message.reservedVolume)}: unable to save balance")
            return
        }

        walletProcessor.apply()

        outgoingEventProcessor.submitReservedCashInOutEvent(ReservedCashInOutEventData(sequenceNumber,
                messageWrapper.messageId!!,
                message.id,
                now,
                walletProcessor.getClientBalanceUpdates(),
                operation,
                walletProcessor,
                asset.accuracy))

        writeResponse(messageWrapper, matchingEngineOperationId, MessageStatus.OK)

        LOGGER.info("Reserved cash in/out operation (${message.id}) for client ${message.clientId}, " +
                "asset ${message.assetId}, amount: ${NumberUtils.roundForPrint(message.reservedVolume)} processed")
    }

    private fun parse(array: ByteArray): ProtocolMessages.ReservedCashInOutOperation {
        return ProtocolMessages.ReservedCashInOutOperation.parseFrom(array)
    }

    override fun parseMessage(messageWrapper: MessageWrapper) {
        val message = parse(messageWrapper.byteArray)
        messageWrapper.messageId = if (message.hasMessageId()) message.messageId else message.id
        messageWrapper.timestamp = message.timestamp
        messageWrapper.parsedMessage = message
        messageWrapper.id = message.id
    }

    fun writeResponse(messageWrapper: MessageWrapper, matchingEngineOperationId: String, status: MessageStatus) {
        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                .setMatchingEngineId(matchingEngineOperationId)
                .setStatus(status.type)
        )
    }


    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus) {
        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                .setStatus(status.type)
        )
    }

    fun writeErrorResponse(messageWrapper: MessageWrapper, matchingEngineOperationId: String, status: MessageStatus, errorMessage: String = StringUtils.EMPTY) {
        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse
                .newBuilder()
                .setMatchingEngineId(matchingEngineOperationId)
                .setStatus(status.type)
                .setStatusReason(errorMessage))
    }

    private fun isCashIn(amount: Double): Boolean {
        return amount > 0
    }

    private fun getMessage(messageWrapper: MessageWrapper): ProtocolMessages.ReservedCashInOutOperation {
        if (messageWrapper.parsedMessage == null) {
            parseMessage(messageWrapper)
        }
        return messageWrapper.parsedMessage!! as ProtocolMessages.ReservedCashInOutOperation
    }

}
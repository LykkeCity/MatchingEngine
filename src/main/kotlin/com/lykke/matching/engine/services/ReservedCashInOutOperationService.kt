package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.balance.BalanceException
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.holders.MessageProcessingStatusHolder
import com.lykke.matching.engine.holders.MessageSequenceNumberHolder
import com.lykke.matching.engine.holders.UUIDHolder
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.outgoing.messages.ClientBalanceUpdate
import com.lykke.matching.engine.outgoing.messages.ReservedCashOperation
import com.lykke.matching.engine.outgoing.messages.v2.builders.EventFactory
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
import java.util.concurrent.BlockingQueue

@Service
class ReservedCashInOutOperationService @Autowired constructor (private val assetsHolder: AssetsHolder,
                                                                private val balancesHolder: BalancesHolder,
                                                                private val reservedCashOperationQueue: BlockingQueue<ReservedCashOperation>,
                                                                private val reservedCashInOutOperationValidator: ReservedCashInOutOperationValidator,
                                                                private val messageProcessingStatusHolder: MessageProcessingStatusHolder,
                                                                private val uuidHolder: UUIDHolder,
                                                                private val messageSequenceNumberHolder: MessageSequenceNumberHolder,
                                                                private val messageSender: MessageSender) : AbstractService {

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

        val walletProcessor = balancesHolder.createWalletProcessor(LOGGER)
        try {
            walletProcessor.preProcess(listOf(operation), allowTrustedClientReservedBalanceOperation = true)
        } catch (e: BalanceException) {
            LOGGER.info("Reserved cash in/out operation (${message.id}) failed due to invalid balance: ${e.message}")
            writeErrorResponse(messageWrapper, matchingEngineOperationId, MessageStatus.LOW_BALANCE, e.message)
            return
        }

        val sequenceNumber = messageSequenceNumberHolder.getNewValue()
        val updated = walletProcessor.persistBalances(messageWrapper.processedMessage, null, null, sequenceNumber)
        messageWrapper.triedToPersist = true
        messageWrapper.persisted = updated
        if (!updated) {
            writeErrorResponse(messageWrapper, matchingEngineOperationId, MessageStatus.RUNTIME)
            LOGGER.info("Reserved cash in/out operation (${message.id}) for client ${message.clientId} asset ${message.assetId}, volume: ${NumberUtils.roundForPrint(message.reservedVolume)}: unable to save balance")
            return
        }
        walletProcessor.apply().sendNotification(message.id, MessageType.RESERVED_CASH_IN_OUT_OPERATION.name, messageWrapper.messageId!!)

        reservedCashOperationQueue.put(ReservedCashOperation(message.id,
                operation.clientId,
                now,
                NumberUtils.setScaleRoundHalfUp(operation.reservedAmount, accuracy).toPlainString(),
                operation.assetId,
                messageWrapper.messageId!!))

        sendEvent(sequenceNumber,
                messageWrapper.messageId!!,
                message.id,
                now,
                walletProcessor.getClientBalanceUpdates(),
                operation)

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


    override fun writeResponse(messageWrapper: MessageWrapper,  status: MessageStatus) {
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

    private fun sendEvent(sequenceNumber: Long,
                          messageId: String,
                          requestId: String,
                          date: Date,
                          clientBalanceUpdates: List<ClientBalanceUpdate>,
                          walletOperation: WalletOperation) {
        val outgoingMessage = EventFactory.createReservedBalanceUpdateEvent(sequenceNumber,
                messageId,
                requestId,
                date,
                MessageType.RESERVED_CASH_IN_OUT_OPERATION,
                clientBalanceUpdates,
                walletOperation)

        messageSender.sendMessage(outgoingMessage)
    }

    private fun getMessage(messageWrapper: MessageWrapper): ProtocolMessages.ReservedCashInOutOperation {
        if (messageWrapper.parsedMessage == null) {
            parseMessage(messageWrapper)
        }
        return messageWrapper.parsedMessage!! as ProtocolMessages.ReservedCashInOutOperation
    }

}
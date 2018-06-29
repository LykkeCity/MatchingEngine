package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.SwapOperation
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.database.CashOperationsDatabaseAccessor
import com.lykke.matching.engine.balance.BalanceException
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageStatus.OK
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.outgoing.messages.CashSwapOperation
import com.lykke.matching.engine.services.validators.CashSwapOperationValidator
import com.lykke.matching.engine.services.validators.impl.ValidationException
import com.lykke.matching.engine.utils.NumberUtils
import com.lykke.matching.engine.utils.order.MessageStatusUtils
import org.apache.commons.lang3.StringUtils
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import java.math.BigDecimal
import java.util.Date
import java.util.LinkedList
import java.util.UUID
import java.util.concurrent.BlockingQueue

@Service
class CashSwapOperationService @Autowired constructor (private val balancesHolder: BalancesHolder,
                                                       private val assetsHolder: AssetsHolder,
                                                       private val cashOperationsDatabaseAccessor: CashOperationsDatabaseAccessor,
                                                       private val cashSwapQueue: BlockingQueue<CashSwapOperation>,
                                                       private val cashSwapOperationValidator: CashSwapOperationValidator) : AbstractService {

    companion object {
        private val LOGGER = Logger.getLogger(CashSwapOperationService::class.java.name)
    }

    override fun processMessage(messageWrapper: MessageWrapper) {
        val message = getMessage(messageWrapper)
        LOGGER.debug("Processing cash swap messageId: ${messageWrapper.messageId}, " +
                "operation (${message.id}) from client ${message.clientId1}, asset ${message.assetId1}," +
                " amount: ${NumberUtils.roundForPrint(message.volume1)} to client ${message.clientId2}, " +
                "asset ${message.assetId2}, amount: ${NumberUtils.roundForPrint(message.volume2)}")

        val operationId = UUID.randomUUID().toString()
        val operation = SwapOperation(operationId, message.id, Date(message.timestamp),
                message.clientId1, message.assetId1, BigDecimal.valueOf(message.volume1),
                message.clientId2, message.assetId2, BigDecimal.valueOf(message.volume2))

        try {
            cashSwapOperationValidator.performValidation(message, operationId)
        } catch (e: ValidationException) {
            writeErrorResponse(messageWrapper, operation, MessageStatusUtils.toMessageStatus(e.validationType), e.message)
            return
        }

        try {
            processSwapOperation(operation, messageWrapper.messageId!!)
        } catch (e: BalanceException) {
            LOGGER.info("Cash swap operation (${message.id}) failed due to invalid balance: ${e.message}")
            writeErrorResponse(messageWrapper, operation, MessageStatus.LOW_BALANCE, e.message)
            return
        } catch (e: Exception) {
            messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                    .setMatchingEngineId(operation.id)
                    .setStatus(MessageStatus.RUNTIME.type)
                    .setStatusReason(e.message))
            return
        }
        cashOperationsDatabaseAccessor.insertSwapOperation(operation)
        cashSwapQueue.add(CashSwapOperation(operation.externalId, operation.dateTime,
                operation.clientId1,
                operation.asset1,
                NumberUtils.setScaleRoundHalfUp(operation.volume1, assetsHolder.getAsset(operation.asset1).accuracy).toPlainString(),
                operation.clientId2,
                operation.asset2,
                NumberUtils.setScaleRoundHalfUp(operation.volume2, assetsHolder.getAsset(operation.asset2).accuracy).toPlainString(),
                messageWrapper.messageId!!))

        messageWrapper
                .writeNewResponse(ProtocolMessages.NewResponse
                .newBuilder()
                .setMatchingEngineId(operation.id)
                .setStatus(OK.type))
        LOGGER.info("Cash swap operation (${message.id}) from client ${message.clientId1}, asset ${message.assetId1}, " +
                "amount: ${NumberUtils.roundForPrint(message.volume1)} to client ${message.clientId2}, asset ${message.assetId2}, " +
                "amount: ${NumberUtils.roundForPrint(message.volume2)} processed")
    }

    private fun processSwapOperation(operation: SwapOperation, messageId: String) {
        val operations = LinkedList<WalletOperation>()

        operations.add(WalletOperation(UUID.randomUUID().toString(), operation.externalId, operation.clientId1, operation.asset1,
                operation.dateTime, -operation.volume1))
        operations.add(WalletOperation(UUID.randomUUID().toString(), operation.externalId, operation.clientId2, operation.asset1,
                operation.dateTime, operation.volume1))

        operations.add(WalletOperation(UUID.randomUUID().toString(), operation.externalId, operation.clientId1, operation.asset2,
                operation.dateTime, operation.volume2))
        operations.add(WalletOperation(UUID.randomUUID().toString(), operation.externalId, operation.clientId2, operation.asset2,
                operation.dateTime, -operation.volume2))

        val walletProcessor = balancesHolder.createWalletProcessor(LOGGER)
        walletProcessor.preProcess(operations)
        val updated = walletProcessor.persistBalances()
        if (!updated) {
            throw Exception("Unable to save balance")
        }
        walletProcessor.apply().sendNotification(operation.externalId, MessageType.CASH_SWAP_OPERATION.name, messageId)
    }

    private fun parse(array: ByteArray): ProtocolMessages.CashSwapOperation {
        return ProtocolMessages.CashSwapOperation.parseFrom(array)
    }

    private fun writeErrorResponse(messageWrapper: MessageWrapper, operation: SwapOperation, status: MessageStatus, errorMessage: String = StringUtils.EMPTY) {
        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse
                .newBuilder()
                .setMatchingEngineId(operation.id)
                .setStatus(status.type)
                .setStatusReason(errorMessage))
    }

    override fun parseMessage(messageWrapper: MessageWrapper) {
        val message = parse(messageWrapper.byteArray)
        messageWrapper.messageId = if (message.hasMessageId()) message.messageId else message.id
        messageWrapper.timestamp = message.timestamp
        messageWrapper.parsedMessage = message
        messageWrapper.id = messageWrapper.id
    }

    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus) {
        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                .setStatus(status.type))
    }

    private fun getMessage(messageWrapper: MessageWrapper): ProtocolMessages.CashSwapOperation {
        if (messageWrapper.parsedMessage == null) {
            parseMessage(messageWrapper)
        }
        return messageWrapper.parsedMessage!! as ProtocolMessages.CashSwapOperation
    }
}
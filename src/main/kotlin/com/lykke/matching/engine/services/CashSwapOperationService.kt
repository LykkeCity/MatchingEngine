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
import com.lykke.matching.engine.outgoing.rabbit.events.CashSwapEvent
import com.lykke.matching.engine.round
import com.lykke.matching.engine.services.validators.CashSwapOperationValidator
import com.lykke.matching.engine.services.validators.impl.ValidationException
import com.lykke.matching.engine.utils.NumberUtils
import com.lykke.matching.engine.utils.order.MessageStatusUtils
import org.apache.commons.lang3.StringUtils
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.ApplicationEventPublisher
import org.springframework.stereotype.Service
import java.util.Date
import java.util.LinkedList
import java.util.UUID

@Service
class CashSwapOperationService @Autowired constructor (private val balancesHolder: BalancesHolder,
                                                       private val assetsHolder: AssetsHolder,
                                                       private val cashOperationsDatabaseAccessor: CashOperationsDatabaseAccessor,
                                                       private val applicationEventPublisher: ApplicationEventPublisher,
                                                       private val cashSwapOperationValidator: CashSwapOperationValidator) : AbstractService {

    companion object {
        private val LOGGER = Logger.getLogger(CashSwapOperationService::class.java.name)
    }

    override fun processMessage(messageWrapper: MessageWrapper) {
        val message = getMessage(messageWrapper)
        LOGGER.debug("Processing cash swap operation (${message.id}) from client ${message.clientId1}, asset ${message.assetId1}, amount: ${NumberUtils.roundForPrint(message.volume1)} " +
                "to client ${message.clientId2}, asset ${message.assetId2}, amount: ${NumberUtils.roundForPrint(message.volume2)}")

        val operationId = UUID.randomUUID().toString()
        val operation = SwapOperation(operationId, message.id, Date(message.timestamp),
                message.clientId1, message.assetId1, message.volume1,
                message.clientId2, message.assetId2, message.volume2)

        try {
            cashSwapOperationValidator.performValidation(message, operationId)
        } catch (e: ValidationException) {
            writeErrorResponse(messageWrapper, operation, MessageStatusUtils.toMessageStatus(e.validationType), e.message)
            return
        }

        try {
            processSwapOperation(operation)
        } catch (e: BalanceException) {
            LOGGER.info("Cash swap operation (${message.id}) failed due to invalid balance: ${e.message}")
            writeErrorResponse(messageWrapper, operation, MessageStatus.LOW_BALANCE, e.message)
            return
        }
        cashOperationsDatabaseAccessor.insertSwapOperation(operation)
        applicationEventPublisher.publishEvent(CashSwapEvent(CashSwapOperation(operation.externalId, operation.dateTime,
                operation.clientId1, operation.asset1, operation.volume1.round(assetsHolder.getAsset(operation.asset1).accuracy),
                operation.clientId2, operation.asset2, operation.volume2.round(assetsHolder.getAsset(operation.asset2).accuracy))))

        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setId(message.id).setMatchingEngineId(operation.id).setStatus(OK.type).build())
        LOGGER.info("Cash swap operation (${message.id}) from client ${message.clientId1}, asset ${message.assetId1}, " +
                "amount: ${NumberUtils.roundForPrint(message.volume1)} to client ${message.clientId2}, asset ${message.assetId2}, " +
                "amount: ${NumberUtils.roundForPrint(message.volume2)} processed")
    }

    private fun processSwapOperation(operation: SwapOperation) {
        val operations = LinkedList<WalletOperation>()

        operations.add(WalletOperation(UUID.randomUUID().toString(), operation.externalId, operation.clientId1, operation.asset1,
                operation.dateTime, -operation.volume1))
        operations.add(WalletOperation(UUID.randomUUID().toString(), operation.externalId, operation.clientId2, operation.asset1,
                operation.dateTime, operation.volume1))

        operations.add(WalletOperation(UUID.randomUUID().toString(), operation.externalId, operation.clientId1, operation.asset2,
                operation.dateTime, operation.volume2))
        operations.add(WalletOperation(UUID.randomUUID().toString(), operation.externalId, operation.clientId2, operation.asset2,
                operation.dateTime, -operation.volume2))

        balancesHolder.createWalletProcessor(LOGGER).preProcess(operations).apply(operation.externalId, MessageType.CASH_SWAP_OPERATION.name)
    }

    private fun parse(array: ByteArray): ProtocolMessages.CashSwapOperation {
        return ProtocolMessages.CashSwapOperation.parseFrom(array)
    }

    private fun writeErrorResponse(messageWrapper: MessageWrapper, operation: SwapOperation, status: MessageStatus, errorMessage: String = StringUtils.EMPTY) {
        val message = getMessage(messageWrapper)
        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse
                .newBuilder()
                .setId(message.id)
                .setMatchingEngineId(operation.id)
                .setStatus(status.type).setStatusReason(errorMessage).build())
    }

    override fun parseMessage(messageWrapper: MessageWrapper) {
        val message = parse(messageWrapper.byteArray)
        messageWrapper.messageId = message.id
        messageWrapper.timestamp = message.timestamp
        messageWrapper.parsedMessage = message
    }

    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus) {
        val message = getMessage(messageWrapper)
        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setId(message.id).setStatus(status.type).build())
    }

    private fun getMessage(messageWrapper: MessageWrapper): ProtocolMessages.CashSwapOperation {
        return messageWrapper.parsedMessage!! as ProtocolMessages.CashSwapOperation
    }
}
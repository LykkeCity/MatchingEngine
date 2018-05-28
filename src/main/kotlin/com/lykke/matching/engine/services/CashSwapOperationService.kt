package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.SwapOperation
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.database.CashOperationsDatabaseAccessor
import com.lykke.matching.engine.balance.BalanceException
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageStatus.LOW_BALANCE
import com.lykke.matching.engine.messages.MessageStatus.OK
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.outgoing.messages.CashSwapOperation
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.utils.RoundingUtils
import org.apache.log4j.Logger
import java.math.BigDecimal
import java.util.Date
import java.util.LinkedList
import java.util.UUID
import java.util.concurrent.BlockingQueue

class CashSwapOperationService(private val balancesHolder: BalancesHolder,
                               private val assetsHolder: AssetsHolder,
                               private val cashOperationsDatabaseAccessor: CashOperationsDatabaseAccessor,
                               private val notificationQueue: BlockingQueue<JsonSerializable>) : AbstractService {

    companion object {
        private val LOGGER = Logger.getLogger(CashSwapOperationService::class.java.name)
    }

    override fun processMessage(messageWrapper: MessageWrapper) {
        val message = messageWrapper.parsedMessage!! as ProtocolMessages.CashSwapOperation
        LOGGER.debug("Processing cash swap operation (${message.id}) from client ${message.clientId1}, asset ${message.assetId1}, amount: ${RoundingUtils.roundForPrint(message.volume1)} " +
                "to client ${message.clientId2}, asset ${message.assetId2}, amount: ${RoundingUtils.roundForPrint(message.volume2)}")

        val operation = SwapOperation(UUID.randomUUID().toString(), message.id, Date(message.timestamp)
                , message.clientId1, message.assetId1, BigDecimal.valueOf(message.volume1)
                , message.clientId2, message.assetId2, BigDecimal.valueOf(message.volume2))

        val balance1 = balancesHolder.getBalance(message.clientId1, message.assetId1)
        val reservedBalance1 = balancesHolder.getReservedBalance(message.clientId1, message.assetId1)
        if (balance1 - reservedBalance1 < operation.volume1) {
            messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setId(message.id).setMatchingEngineId(operation.id)
                    .setStatus(LOW_BALANCE.type).setStatusReason("ClientId:${message.clientId1},asset:${message.assetId1}, volume:${message.volume1}").build())
            LOGGER.info("Cash swap operation failed due to low balance: ${operation.clientId1}, ${operation.volume1} ${operation.asset1}")
            return
        }

        val balance2 = balancesHolder.getBalance(message.clientId2, message.assetId2)
        val reservedBalance2 = balancesHolder.getReservedBalance(message.clientId2, message.assetId2)
        if (balance2 - reservedBalance2 < operation.volume1) {
            messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setId(message.id).setMatchingEngineId(operation.id)
                    .setStatus(LOW_BALANCE.type).setStatusReason("ClientId:${message.clientId2},asset:${message.assetId2}, volume:${message.volume2}").build())
            LOGGER.info("Cash swap operation failed due to low balance: ${operation.clientId2}, ${operation.volume2} ${operation.asset2}")
            return
        }

        try {
            processSwapOperation(operation)
        } catch (e: BalanceException) {
            LOGGER.info("Cash swap operation (${message.id}) failed due to invalid balance: ${e.message}")
            messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setId(message.id).setMatchingEngineId(operation.id)
                    .setStatus(MessageStatus.LOW_BALANCE.type).setStatusReason(e.message).build())
            return
        }
        cashOperationsDatabaseAccessor.insertSwapOperation(operation)
        notificationQueue.put(CashSwapOperation(operation.externalId, operation.dateTime,
                operation.clientId1, operation.asset1,
                RoundingUtils.setScaleRoundHalfUp(operation.volume1, assetsHolder.getAsset(operation.asset1).accuracy).toPlainString(),
                operation.clientId2, operation.asset2,
                RoundingUtils.setScaleRoundHalfUp(operation.volume2, assetsHolder.getAsset(operation.asset2).accuracy).toPlainString()))

        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setId(message.id).setMatchingEngineId(operation.id).setStatus(OK.type).build())
        LOGGER.info("Cash swap operation (${message.id}) from client ${message.clientId1}, asset ${message.assetId1}, amount: ${RoundingUtils.roundForPrint(message.volume1)} " +
                "to client ${message.clientId2}, asset ${message.assetId2}, amount: ${RoundingUtils.roundForPrint(message.volume2)} processed")
    }

    private fun parse(array: ByteArray): ProtocolMessages.CashSwapOperation {
        return ProtocolMessages.CashSwapOperation.parseFrom(array)
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

    override fun parseMessage(messageWrapper: MessageWrapper) {
        val message = parse(messageWrapper.byteArray)
        messageWrapper.messageId = message.id
        messageWrapper.timestamp = message.timestamp
        messageWrapper.parsedMessage = message
    }

    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus) {
        val message = messageWrapper.parsedMessage!! as ProtocolMessages.CashSwapOperation
        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setId(message.id).setStatus(status.type).build())
    }
}
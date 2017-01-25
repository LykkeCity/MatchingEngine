package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.SwapOperation
import com.lykke.matching.engine.database.WalletDatabaseAccessor
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.outgoing.messages.CashSwapOperation
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.round
import com.lykke.matching.engine.utils.RoundingUtils
import org.apache.log4j.Logger
import java.util.Date
import java.util.UUID
import java.util.concurrent.BlockingQueue

class CashSwapOperationService(private val balancesHolder: BalancesHolder,
                           private val assetsHolder: AssetsHolder,
                           private val walletDatabaseAccessor: WalletDatabaseAccessor,
                           private val notificationQueue: BlockingQueue<JsonSerializable>): AbsractService<ProtocolMessages.CashOperation> {

    companion object {
        val LOGGER = Logger.getLogger(CashTransferOperationService::class.java.name)
    }

    private var messagesCount: Long = 0

    override fun processMessage(messageWrapper: MessageWrapper) {
        val message = parse(messageWrapper.byteArray)
        LOGGER.debug("Processing cash swap operation (${message.id}) from client ${message.clientId1}, asset ${message.assetId1}, amount: ${RoundingUtils.roundForPrint(message.volume1)} " +
                "to client ${message.clientId2}, asset ${message.assetId2}, amount: ${RoundingUtils.roundForPrint(message.volume2)}")

        val operation = SwapOperation(UUID.randomUUID().toString(), message.id, Date(message.timestamp)
                , message.clientId1, message.assetId1, message.volume1
                , message.clientId2, message.assetId2, message.volume2)

        val balance1 = balancesHolder.getBalance(message.clientId1, message.assetId1)
        if (balance1 < operation.volume1) {
            messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setId(message.id).setMatchingEngineId(operation.id).build())
            LOGGER.info("Cash swap operation failed due to low balance: ${operation.clientId1}, ${operation.volume1} ${operation.asset1}")
            return
        }

        val balance2 = balancesHolder.getBalance(message.clientId2, message.assetId2)
        if (balance2 < operation.volume1) {
            messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setId(message.id).setMatchingEngineId(operation.id).build())
            LOGGER.info("Cash swap operation failed due to low balance: ${operation.clientId2}, ${operation.volume2} ${operation.asset2}")
            return
        }

        processSwapOperation(operation)
        walletDatabaseAccessor.insertSwapOperation(operation)
        notificationQueue.put(CashSwapOperation(operation.externalId, operation.dateTime,
                operation.clientId1, operation.asset1, operation.volume1.round(assetsHolder.getAsset(operation.asset1).accuracy),
                operation.clientId2, operation.asset2, operation.volume2.round(assetsHolder.getAsset(operation.asset2).accuracy)))

        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setId(message.id).setMatchingEngineId(operation.id).build())
        LOGGER.info("Cash swap operation (${message.id}) from client ${message.clientId1}, asset ${message.assetId1}, amount: ${RoundingUtils.roundForPrint(message.volume1)} " +
                "to client ${message.clientId2}, asset ${message.assetId2}, amount: ${RoundingUtils.roundForPrint(message.volume2)} processed")
    }

    private fun parse(array: ByteArray): ProtocolMessages.CashSwapOperation {
        return ProtocolMessages.CashSwapOperation.parseFrom(array)
    }

    fun processSwapOperation(operation: SwapOperation) {
        balancesHolder.addBalance(operation.clientId1, operation.asset1, -operation.volume1)
        balancesHolder.addBalance(operation.clientId2, operation.asset1, operation.volume1)

        balancesHolder.addBalance(operation.clientId1, operation.asset2, operation.volume2)
        balancesHolder.addBalance(operation.clientId2, operation.asset2, -operation.volume2)
    }
}
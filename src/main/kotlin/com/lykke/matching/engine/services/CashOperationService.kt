package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.ExternalCashOperation
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.database.BackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.WalletDatabaseAccessor
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.logging.AMOUNT
import com.lykke.matching.engine.logging.ASSET
import com.lykke.matching.engine.logging.BUSSINES_ID
import com.lykke.matching.engine.logging.CLIENT_ID
import com.lykke.matching.engine.logging.KeyValue
import com.lykke.matching.engine.logging.Line
import com.lykke.matching.engine.logging.ME_CASH_OPERATION
import com.lykke.matching.engine.logging.MetricsLogger
import com.lykke.matching.engine.logging.SEND_TO_BITCOIN
import com.lykke.matching.engine.logging.TIMESTAMP
import com.lykke.matching.engine.logging.UID
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.queue.transaction.CashIn
import com.lykke.matching.engine.queue.transaction.CashOut
import com.lykke.matching.engine.queue.transaction.Transaction
import com.lykke.matching.engine.utils.RoundingUtils
import org.apache.log4j.Logger
import java.time.LocalDateTime
import java.util.Date
import java.util.UUID
import java.util.concurrent.BlockingQueue

class CashOperationService(private val walletDatabaseAccessor: WalletDatabaseAccessor,
                           private val backOfficeDatabaseAccessor: BackOfficeDatabaseAccessor,
                           private val backendQueue: BlockingQueue<Transaction>,
                           private val balancesHolder: BalancesHolder): AbsractService<ProtocolMessages.CashOperation> {

    companion object {
        val LOGGER = Logger.getLogger(CashOperationService::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    private val balances = walletDatabaseAccessor.loadBalances()
    private val wallets = walletDatabaseAccessor.loadWallets()
    private val assets = backOfficeDatabaseAccessor.loadAssets()
    private val assetPairs = walletDatabaseAccessor.loadAssetPairs()

    private var messagesCount: Long = 0

    override fun processMessage(messageWrapper: MessageWrapper) {
        val message = parse(messageWrapper.byteArray)
        LOGGER.debug("Processing cash operation (${message.bussinesId}) for client ${message.clientId}, asset ${message.assetId}, amount: ${RoundingUtils.roundForPrint(message.amount)}, sendToBitcoin: ${message.sendToBitcoin}")

        val externalCashOperation = walletDatabaseAccessor.loadExternalCashOperation(message.clientId, message.bussinesId)
        if (externalCashOperation != null) {
            messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder().setUid(message.uid).setBussinesId(message.bussinesId)
                    .setRecordId(externalCashOperation.cashOperationId).build())
            LOGGER.debug("Cash operation (${message.bussinesId}) for client ${message.clientId}, asset ${message.assetId}, amount: ${RoundingUtils.roundForPrint(message.amount)}, sendToBitcoin: ${message.sendToBitcoin} already processed")
            return
        }

        val operation = WalletOperation(UUID.randomUUID().toString(), message.uid.toString(), message.clientId, message.assetId,
                Date(message.timestamp), message.amount, if (message.sendToBitcoin) UUID.randomUUID().toString() else null)
        balancesHolder.processWalletOperations(listOf(operation))

        walletDatabaseAccessor.insertExternalCashOperation(ExternalCashOperation(operation.clientId, message.bussinesId, operation.id))

        if (message.sendToBitcoin) {
            val cashOperation = if (operation.amount > 0.0)
                CashIn(operation.transactionId!!, clientId = operation.clientId, Amount = operation.amount, Currency = operation.assetId, cashOperationId = operation.id)
            else
                CashOut(operation.transactionId!!, clientId = operation.clientId, Amount = -operation.amount, Currency = operation.assetId, cashOperationId = operation.id)

            backendQueue.put(cashOperation)
        }

        messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder().setUid(message.uid).setBussinesId(message.bussinesId)
                .setRecordId(operation.id).build())
        LOGGER.debug("Cash operation (${message.bussinesId}) for client ${message.clientId}, asset ${message.assetId}, amount: ${RoundingUtils.roundForPrint(message.amount)}, sendToBitcoin: ${message.sendToBitcoin} processed")

        METRICS_LOGGER.log(Line(ME_CASH_OPERATION, arrayOf(
                KeyValue(UID, message.uid.toString()),
                KeyValue(BUSSINES_ID, message.bussinesId),
                KeyValue(TIMESTAMP, LocalDateTime.now().format(MetricsLogger.DATE_TIME_FORMATTER)),
                KeyValue(CLIENT_ID, message.clientId),
                KeyValue(ASSET, message.assetId),
                KeyValue(AMOUNT, message.amount.toString()),
                KeyValue(SEND_TO_BITCOIN, message.sendToBitcoin.toString())
        )))
        METRICS_LOGGER.log(KeyValue(ME_CASH_OPERATION, (++messagesCount).toString()))
    }

    private fun parse(array: ByteArray): ProtocolMessages.CashOperation {
        return ProtocolMessages.CashOperation.parseFrom(array)
    }

    fun getAsset(assetId: String): Asset {
        var asset = assets[assetId]
        if (asset == null) {
            asset = backOfficeDatabaseAccessor.loadAsset(assetId)
            if (asset != null) {
                assets[assetId] = asset
            }
        }

        if (asset == null) {
            throw Exception("Unable to find asset pair $assetId")
        }

        return asset
    }

    fun getAssetPair(assetPairId: String): AssetPair? {
        var assetPair = assetPairs[assetPairId]
        if (assetPair == null) {
            assetPair = walletDatabaseAccessor.loadAssetPair(assetPairId)
            if (assetPair != null) {
                assetPairs[assetPairId] = assetPair
            }
        }

        return assetPair
    }
}
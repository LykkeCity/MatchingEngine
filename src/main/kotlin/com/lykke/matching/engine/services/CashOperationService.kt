package com.lykke.matching.engine.services

import com.lykke.matching.engine.balance.BalanceException
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.database.WalletDatabaseAccessor
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.utils.NumberUtils
import com.lykke.utils.logging.MetricsLogger
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import java.util.Date
import java.util.UUID

@Service
class CashOperationService @Autowired constructor (private val walletDatabaseAccessor: WalletDatabaseAccessor,
                                                   private val balancesHolder: BalancesHolder,
                                                   private val applicationSettingsCache: ApplicationSettingsCache,
                                                   private val assetsHolder: AssetsHolder): AbstractService {
    companion object {
        val LOGGER = Logger.getLogger(CashOperationService::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    override fun processMessage(messageWrapper: MessageWrapper) {
        val message = getMessage(messageWrapper)
        LOGGER.debug("Processing cash operation (${message.bussinesId}) for client ${message.clientId}, asset ${message.assetId}, amount: ${NumberUtils.roundForPrint(message.amount)}")

        if(!performValidation(messageWrapper)) {
            return
        }

        val operation = WalletOperation(UUID.randomUUID().toString(), message.uid.toString(), message.clientId, message.assetId,
                Date(message.timestamp), message.amount, 0.0)

        try {
            balancesHolder.createWalletProcessor(LOGGER).preProcess(listOf(operation)).apply(message.uid.toString(), MessageType.CASH_OPERATION.name)
        } catch (e: BalanceException) {
            LOGGER.info("Unable to process cash operation (${message.bussinesId}): ${e.message}")
            messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder().setUid(message.uid).setBussinesId(message.bussinesId).build())
            return
        }

        messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder().setUid(message.uid).setBussinesId(message.bussinesId).setRecordId(operation.id).build())
        LOGGER.debug("Cash operation (${message.bussinesId}) for client ${message.clientId}, asset ${message.assetId}, amount: ${NumberUtils.roundForPrint(message.amount)} processed")
    }

    private fun performValidation(messageWrapper: MessageWrapper): Boolean {
        val validations = arrayOf({isAssetEnabled(messageWrapper)}, {isBalanceValid(messageWrapper)}, {isAccuracyValid(messageWrapper)})

        val failedValidation = validations.find { function: () -> Boolean -> !function() }

        return failedValidation == null
    }

    private fun isBalanceValid(messageWrapper: MessageWrapper): Boolean {
        val message = getMessage(messageWrapper)
        if (message.amount < 0) {
            val balance = balancesHolder.getBalance(message.clientId, message.assetId)
            val reservedBalance = balancesHolder.getReservedBalance(message.clientId, message.assetId)
            if (balance - reservedBalance < Math.abs(message.amount)) {
                LOGGER.info("""Cash out operation (${message.uid})
                        for client ${message.clientId} asset ${message.assetId},
                        volume: ${NumberUtils.roundForPrint(message.amount)}:
                        low balance $balance, reserved balance $reservedBalance""")
                writeErrorResponse(messageWrapper)
                return false
            }
        }

        return true
    }

    private fun isAccuracyValid(messageWrapper: MessageWrapper): Boolean {
        val message = getMessage(messageWrapper)

        val volumeValid = NumberUtils.isScaleSmallerOrEqual(message.amount, assetsHolder.getAsset(message.assetId).accuracy)

        if (!volumeValid) {
            LOGGER.info("amount accuracy is invalid clientId: ${message.clientId}, amount  $message.amount")
            writeErrorResponse(messageWrapper)
        }

        return volumeValid
    }

    private fun isAssetEnabled(messageWrapper: MessageWrapper): Boolean {
        val message = getMessage(messageWrapper)
        if (message.amount < 0 && applicationSettingsCache.isAssetDisabled(message.assetId)) {
            LOGGER.info("""Cash out operation (${message.uid})
                    |for client ${message.clientId} asset ${message.assetId},
                    |volume: ${NumberUtils.roundForPrint(message.amount)}: disabled asset""".trimMargin())
            writeErrorResponse(messageWrapper)
            return false
        }

        return true
    }

    private fun parse(array: ByteArray): ProtocolMessages.CashOperation {
        return ProtocolMessages.CashOperation.parseFrom(array)
    }

    override fun parseMessage(messageWrapper: MessageWrapper) {
        val message = parse(messageWrapper.byteArray)
        messageWrapper.messageId = message.bussinesId
        messageWrapper.timestamp = message.timestamp
        messageWrapper.parsedMessage = message
    }

    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus) {
        val message = getMessage(messageWrapper)
        messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder().setUid(message.uid).setBussinesId(message.bussinesId).build())
    }

    fun writeErrorResponse(messageWrapper: MessageWrapper) {
        val message = getMessage(messageWrapper)
        messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder()
                .setUid(message.uid)
                .setBussinesId(message.bussinesId).build())
    }

    private fun getMessage(messageWrapper: MessageWrapper) =
            messageWrapper.parsedMessage!! as ProtocolMessages.CashOperation
}
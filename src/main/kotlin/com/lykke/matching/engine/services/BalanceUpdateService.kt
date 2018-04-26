package com.lykke.matching.engine.services

import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.outgoing.messages.BalanceUpdate
import com.lykke.matching.engine.outgoing.messages.ClientBalanceUpdate
import com.lykke.matching.engine.utils.NumberUtils
import com.lykke.utils.logging.MetricsLogger
import org.apache.log4j.Logger
import java.util.Date

class BalanceUpdateService(private val balancesHolder: BalancesHolder, private val assetsHolder: AssetsHolder): AbstractService {

    companion object {
        val LOGGER = Logger.getLogger(BalanceUpdateService::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    private var messagesCount: Long = 0

    override fun processMessage(messageWrapper: MessageWrapper) {
        if (messageWrapper.parsedMessage == null) {
            parseMessage(messageWrapper)
        }
        if (messageWrapper.type == MessageType.OLD_BALANCE_UPDATE.type) {
            val message = messageWrapper.parsedMessage!! as ProtocolMessages.OldBalanceUpdate
            LOGGER.debug("Processing holders update for client ${message.clientId}, asset ${message.assetId}, amount: ${NumberUtils.roundForPrint(message.amount)}")


            val balance = balancesHolder.getBalance(message.clientId, message.assetId)
            val reservedBalance = balancesHolder.getReservedBalance(message.clientId, message.assetId)

            balancesHolder.updateBalance(message.clientId, message.assetId, message.amount)
            balancesHolder.sendBalanceUpdate(BalanceUpdate(message.uid.toString(), MessageType.BALANCE_UPDATE.name, Date(), listOf(ClientBalanceUpdate(message.clientId, message.assetId, balance, message.amount, reservedBalance, reservedBalance))))

            messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder().setUid(message.uid).build())
            LOGGER.debug("Balance updated for client ${message.clientId}, asset ${message.assetId}, amount: ${NumberUtils.roundForPrint(message.amount)}")
        } else {
            val message = messageWrapper.parsedMessage!! as ProtocolMessages.BalanceUpdate
            LOGGER.debug("Processing holders update for client ${message.clientId}, asset ${message.assetId}, amount: ${NumberUtils.roundForPrint(message.amount)}")

            val balance = balancesHolder.getBalance(message.clientId, message.assetId)
            val reservedBalance = balancesHolder.getReservedBalance(message.clientId, message.assetId)

            if (!performValidation(messageWrapper, message)) {
                return
            }

            balancesHolder.updateBalance(message.clientId, message.assetId, message.amount)
            balancesHolder.sendBalanceUpdate(BalanceUpdate(message.uid, MessageType.BALANCE_UPDATE.name, Date(), listOf(ClientBalanceUpdate(message.clientId, message.assetId, balance, message.amount, reservedBalance, reservedBalance))))

            messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setId(message.uid).setStatus(MessageStatus.OK.type).build())
            LOGGER.debug("Balance updated for client ${message.clientId}, asset ${message.assetId}, amount: ${NumberUtils.roundForPrint(message.amount)}")
        }
    }

    private fun performValidation(messageWrapper: MessageWrapper, message: ProtocolMessages.BalanceUpdate): Boolean {
        val validations = arrayOf({isBalanceValid(messageWrapper, message)},
                {isAmountAccuracyValid(messageWrapper, message)})

        val failedValidation = validations.find { function: () -> Boolean -> !function() }

        return failedValidation == null
    }

    private fun isBalanceValid(messageWrapper: MessageWrapper, message: ProtocolMessages.BalanceUpdate): Boolean {
        val reservedBalance = balancesHolder.getReservedBalance(message.clientId, message.assetId)

        if (reservedBalance > message.amount) {
            messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setId(message.uid).setStatus(MessageStatus.BALANCE_LOWER_THAN_RESERVED.type).build())
            LOGGER.info("""Balance (client ${message.clientId},
                |asset ${message.assetId}, ${NumberUtils.roundForPrint(message.amount)})
                |is lower that reserved balance ${NumberUtils.roundForPrint(reservedBalance)}""".trimMargin())
            return false
        }

        return true
    }

    private fun isAmountAccuracyValid(messageWrapper: MessageWrapper, message: ProtocolMessages.BalanceUpdate): Boolean {
        val amount = message.amount
        val assetId = message.assetId
        val amountAccuracyValid = NumberUtils.isScaleSmallerOrEqual(amount, assetsHolder.getAsset(assetId).accuracy)

        if (!amountAccuracyValid) {
            LOGGER.info("Amount accuracy invalid client: ${message.clientId}, asset: $assetId, amount $amount")
            messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                    .setId(message.uid)
                    .setStatus(MessageStatus.INVALID_VOLUME_ACCURACY.type).build())
            return false
        }

        return true
    }

    private fun parse(array: ByteArray): ProtocolMessages.BalanceUpdate {
        return ProtocolMessages.BalanceUpdate.parseFrom(array)
    }

    private fun parseOld(array: ByteArray): ProtocolMessages.OldBalanceUpdate {
        return ProtocolMessages.OldBalanceUpdate.parseFrom(array)
    }

    override fun parseMessage(messageWrapper: MessageWrapper) {
        if (messageWrapper.type == MessageType.OLD_BALANCE_UPDATE.type) {
            val message =  parseOld(messageWrapper.byteArray)
            messageWrapper.messageId = message.uid.toString()
            messageWrapper.timestamp = Date().time
            messageWrapper.parsedMessage = message
        } else {
            val message =  parse(messageWrapper.byteArray)
            messageWrapper.messageId = message.uid
            messageWrapper.timestamp = Date().time
            messageWrapper.parsedMessage = message
        }
    }

    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus) {
        if (messageWrapper.type == MessageType.OLD_BALANCE_UPDATE.type) {
            messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder().setUid(messageWrapper.messageId!!.toLong()).build())
        } else {
            messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setId(messageWrapper.messageId!!).setStatus(status.type).build())
        }
    }
}
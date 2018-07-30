package com.lykke.matching.engine.services

import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.outgoing.messages.BalanceUpdate
import com.lykke.matching.engine.outgoing.messages.ClientBalanceUpdate
import com.lykke.matching.engine.services.validators.BalanceUpdateValidator
import com.lykke.matching.engine.services.validators.impl.ValidationException
import com.lykke.matching.engine.utils.NumberUtils
import com.lykke.matching.engine.utils.order.MessageStatusUtils
import com.lykke.utils.logging.MetricsLogger
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import java.math.BigDecimal
import java.util.Date

@Service
class BalanceUpdateService @Autowired constructor (private val balancesHolder: BalancesHolder,
                                                   private val balanceUpdateValidator: BalanceUpdateValidator): AbstractService {

    companion object {
        private val LOGGER = Logger.getLogger(BalanceUpdateService::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    override fun processMessage(messageWrapper: MessageWrapper) {
        if (messageWrapper.parsedMessage == null) {
            parseMessage(messageWrapper)
        }
        if (messageWrapper.type == MessageType.OLD_BALANCE_UPDATE.type) {
            val message = getMessage(messageWrapper)
            LOGGER.debug("Processing holders update messageId: ${messageWrapper.messageId} for client ${message.clientId}, " +
                    "asset ${message.assetId}, amount: ${NumberUtils.roundForPrint(message.amount)}")


            val balance = balancesHolder.getBalance(message.clientId, message.assetId)
            val reservedBalance = balancesHolder.getReservedBalance(message.clientId, message.assetId)

            val updated = balancesHolder.updateBalance(messageWrapper.processedMessage(),
                    null,
                    message.clientId,
                    message.assetId,
                    BigDecimal.valueOf(message.amount))
            messageWrapper.triedToPersist = true
            messageWrapper.persisted = updated
            if (updated) {
                balancesHolder.sendBalanceUpdate(BalanceUpdate(message.uid.toString(),
                        MessageType.BALANCE_UPDATE.name, Date(),
                        listOf(ClientBalanceUpdate(message.clientId, message.assetId, balance, BigDecimal.valueOf(message.amount), reservedBalance, reservedBalance)), messageWrapper.messageId!!))
            }
            messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder())

            LOGGER.debug("Balance updated for client ${message.clientId}, asset ${message.assetId}, amount: ${NumberUtils.roundForPrint(message.amount)}")
        } else {
            val message = messageWrapper.parsedMessage!! as ProtocolMessages.BalanceUpdate
            LOGGER.debug("Processing holders update for client ${message.clientId}, asset ${message.assetId}, amount: ${NumberUtils.roundForPrint(message.amount)}")

            val balance = balancesHolder.getBalance(message.clientId, message.assetId)
            val reservedBalance = balancesHolder.getReservedBalance(message.clientId, message.assetId)

            try {
                balanceUpdateValidator.performValidation(message)
            } catch (e: ValidationException) {
                messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                        .setStatus(MessageStatusUtils.toMessageStatus(e.validationType).type))
                return
            }

            val updated = balancesHolder.updateBalance(messageWrapper.processedMessage(),
                    null,
                    message.clientId,
                    message.assetId,
                    BigDecimal.valueOf(message.amount))
            messageWrapper.triedToPersist = true
            messageWrapper.persisted = updated
            if (!updated) {
                messageWrapper.writeNewResponse(
                        ProtocolMessages.NewResponse.newBuilder()
                                .setStatus(MessageStatus.RUNTIME.type)
                                .setStatusReason("Unable to save balance"))
                LOGGER.info("Unable to save balance (client ${message.clientId}, asset ${message.assetId}, ${NumberUtils.roundForPrint(message.amount)})")
                return
            }
            balancesHolder.sendBalanceUpdate(BalanceUpdate(message.uid,
                    MessageType.BALANCE_UPDATE.name,
                    Date(),
                    listOf(ClientBalanceUpdate(message.clientId, message.assetId, balance, BigDecimal.valueOf(message.amount), reservedBalance, reservedBalance)),
                    messageWrapper.messageId!!))

            messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setStatus(MessageStatus.OK.type))
            LOGGER.debug("Balance updated for client ${message.clientId}, asset ${message.assetId}, amount: ${NumberUtils.roundForPrint(message.amount)}")
        }
    }

    private fun getMessage(messageWrapper: MessageWrapper) =
            messageWrapper.parsedMessage!! as ProtocolMessages.OldBalanceUpdate

    private fun parse(array: ByteArray): ProtocolMessages.BalanceUpdate {
        return ProtocolMessages.BalanceUpdate.parseFrom(array)
    }

    private fun parseOld(array: ByteArray): ProtocolMessages.OldBalanceUpdate {
        return ProtocolMessages.OldBalanceUpdate.parseFrom(array)
    }

    override fun parseMessage(messageWrapper: MessageWrapper) {
        if (messageWrapper.type == MessageType.OLD_BALANCE_UPDATE.type) {
            val message =  parseOld(messageWrapper.byteArray)
            messageWrapper.messageId = if(message.hasMessageId()) message.messageId else  message.uid.toString()
            messageWrapper.id = message.uid.toString()
            messageWrapper.parsedMessage = message
        } else {
            val message =  parse(messageWrapper.byteArray)
            messageWrapper.messageId = if(message.hasMessageId()) message.messageId else  message.uid
            messageWrapper.parsedMessage = message
            messageWrapper.id = message.uid
        }

        messageWrapper.timestamp = Date().time
    }

    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus) {
        if (messageWrapper.type == MessageType.OLD_BALANCE_UPDATE.type) {
            messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder())
        } else {
            messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                     .setStatus(status.type))
        }
    }
}
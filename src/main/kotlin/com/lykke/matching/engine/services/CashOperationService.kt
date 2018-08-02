package com.lykke.matching.engine.services

import com.lykke.matching.engine.balance.BalanceException
import com.lykke.matching.engine.daos.context.CashOperationContext
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.services.validators.business.CashOperationBusinessValidator
import com.lykke.matching.engine.services.validators.impl.ValidationException
import com.lykke.matching.engine.utils.NumberUtils
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

@Service
class CashOperationService @Autowired constructor (private val balancesHolder: BalancesHolder,
                                                   private val cashOperationBusinessValidator: CashOperationBusinessValidator): AbstractService {
    companion object {
        private val LOGGER = Logger.getLogger(CashOperationService::class.java.name)
    }

    override fun processMessage(messageWrapper: MessageWrapper) {

        val context = messageWrapper.context as CashOperationContext
        val walletOperation = context.walletOperation

        LOGGER.debug("Processing cash messageId: ${context.messageId}," +
                " operation (${context.businessId}),for client ${context.clientId}, " +
                "asset ${walletOperation.assetId}, amount: ${NumberUtils.roundForPrint(walletOperation.amount)}")

        try {
            cashOperationBusinessValidator.performValidation(context)
        } catch (e: ValidationException) {
            writeErrorResponse(messageWrapper)
            return
        }

        val walletProcessor = balancesHolder.createWalletProcessor(LOGGER)
        try {
            walletProcessor.preProcess(listOf(walletOperation))
        } catch (e: BalanceException) {
            LOGGER.info("Unable to process cash operation (${context.businessId}): ${e.message}")
            messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder()
                    .setBussinesId(context.businessId))
            return
        }

        val updated = walletProcessor.persistBalances(messageWrapper.processedMessage(), null)
        messageWrapper.processedMessagePersisted = true
        if (updated) {
            walletProcessor.apply().sendNotification(context.uid, MessageType.CASH_OPERATION.name, context.messageId)
        }

        messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder()
                .setBussinesId(context.businessId)
                .setRecordId(walletOperation.id))
        LOGGER.debug("Cash operation (${context.businessId}) for client ${context.clientId}, asset ${walletOperation.assetId}, amount: ${NumberUtils.roundForPrint(walletOperation.amount)} processed")
    }

    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus) {
        val context = getContext(messageWrapper)
        messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder()
                .setBussinesId(context.businessId))
    }

    fun writeErrorResponse(messageWrapper: MessageWrapper) {
        val context = getContext(messageWrapper)
        messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder()
                .setBussinesId(context.businessId))
    }

    fun getContext(messageWrapper: MessageWrapper): CashOperationContext {
        return messageWrapper.context as CashOperationContext
    }

    override fun parseMessage(messageWrapper: MessageWrapper) {
        //do nothing
    }
}
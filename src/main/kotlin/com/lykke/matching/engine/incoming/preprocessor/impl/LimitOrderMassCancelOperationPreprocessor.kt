package com.lykke.matching.engine.incoming.preprocessor.impl

import com.lykke.matching.engine.daos.DisabledFunctionalityRule
import com.lykke.matching.engine.daos.context.LimitOrderMassCancelOperationContext
import com.lykke.matching.engine.holders.MessageProcessingStatusHolder
import com.lykke.matching.engine.incoming.parsers.impl.LimitOrderMassCancelOperationContextParser
import com.lykke.matching.engine.incoming.preprocessor.MessagePreprocessor
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingQueue

@Component
class LimitOrderMassCancelOperationPreprocessor(val preProcessedMessageQueue: BlockingQueue<MessageWrapper>,
                                                val limitOrderMassCancelOperationContextParser: LimitOrderMassCancelOperationContextParser,
                                                val messageProcessingStatusHolder: MessageProcessingStatusHolder) :
        MessagePreprocessor {

    override fun preProcess(messageWrapper: MessageWrapper) {
        val parsedData = limitOrderMassCancelOperationContextParser.parse(messageWrapper)
        val context = parsedData.messageWrapper.context as LimitOrderMassCancelOperationContext

        if (!messageProcessingStatusHolder.isMessageProcessingEnabled(DisabledFunctionalityRule(null, context.assetPairId, MessageType.LIMIT_ORDER_MASS_CANCEL))) {
            writeResponse(messageWrapper, MessageStatus.MESSAGE_PROCESSING_DISABLED)
            return
        }

        preProcessedMessageQueue.put(parsedData.messageWrapper)
    }

    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus, message: String?) {
        val responseBuilder = ProtocolMessages.NewResponse.newBuilder()
                .setStatus(status.type)

        message?.let { responseBuilder.setStatusReason(it) }
        messageWrapper.writeNewResponse(responseBuilder)
    }
}

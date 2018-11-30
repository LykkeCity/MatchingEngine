package com.lykke.matching.engine.incoming.preprocessor.impl

import com.lykke.matching.engine.incoming.parsers.impl.LimitOrderMassCancelOperationContextParser
import com.lykke.matching.engine.incoming.preprocessor.MessagePreprocessor
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingQueue

@Component
class LimitOrderMassCancelOperationPreprocessor(val preProcessedMessageQueue: BlockingQueue<MessageWrapper>,
                                                val limitOrderMassCancelOperationContextParser: LimitOrderMassCancelOperationContextParser) :
        MessagePreprocessor {

    override fun preProcess(messageWrapper: MessageWrapper) {
        val parsedData = limitOrderMassCancelOperationContextParser.parse(messageWrapper)
        preProcessedMessageQueue.put(parsedData.messageWrapper)
    }

    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus, message: String?) {
        val responseBuilder = ProtocolMessages.NewResponse.newBuilder()
                .setStatus(status.type)

        message?.let { responseBuilder.setStatusReason(it) }
        messageWrapper.writeNewResponse(responseBuilder)
    }
}

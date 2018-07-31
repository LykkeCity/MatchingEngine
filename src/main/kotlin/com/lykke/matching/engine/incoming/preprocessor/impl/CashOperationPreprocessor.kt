package com.lykke.matching.engine.incoming.preprocessor.impl

import com.lykke.matching.engine.incoming.parsers.ContextParser
import com.lykke.matching.engine.incoming.parsers.data.CashOperationParsedData
import com.lykke.matching.engine.incoming.preprocessor.MessagePreprocessor
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageWrapper
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingQueue
import javax.annotation.PostConstruct

@Component
class CashOperationPreprocessor(val cashOperationInputQueue: BlockingQueue<MessageWrapper>,
                                val preProcessedMessageQueue: BlockingQueue<MessageWrapper>,
                                val cashoperationParser: ContextParser<CashOperationParsedData>): MessagePreprocessor, Thread(CashOperationPreprocessor::class.java.name) {
    override fun preProcess(messageWrapper: MessageWrapper) {
        val parsedMessageWrapper = cashoperationParser.parse(messageWrapper)

        preProcessedMessageQueue.put(parsedMessageWrapper.messageWrapper)
    }

    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun parseMessage(messageWrapper: MessageWrapper) {
        // nothing to do
    }

    @PostConstruct
    fun init() {
        while(true) {

        }
    }
}
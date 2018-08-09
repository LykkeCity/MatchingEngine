package com.lykke.matching.engine.incoming

import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingQueue

@Component
class MessageRouter(
        private val limitOrderInputQueue: BlockingQueue<MessageWrapper>,
        val cashInOutQueue: BlockingQueue<MessageWrapper>,
        val cashTransferQueue: BlockingQueue<MessageWrapper>,
        val preProcessedMessageQueue: BlockingQueue<MessageWrapper>
) {
    fun process(wrapper: MessageWrapper) {
        when(wrapper.type) {
            MessageType.CASH_IN_OUT_OPERATION.type -> cashInOutQueue.put(wrapper)
            MessageType.CASH_TRANSFER_OPERATION.type -> cashTransferQueue.put(wrapper)
            MessageType.OLD_LIMIT_ORDER.type,
            MessageType.LIMIT_ORDER.type -> limitOrderInputQueue.put(wrapper)

            else -> preProcessedMessageQueue.put(wrapper)
        }
    }
}
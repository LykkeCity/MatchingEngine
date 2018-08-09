package com.lykke.matching.engine.incoming

import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingQueue

@Component
class MessageRouter(
        val cashInOutInputQueue: BlockingQueue<MessageWrapper>,
        val cashTransferInputQueue: BlockingQueue<MessageWrapper>,
        private val limitOrderCancelInputQueue: BlockingQueue<MessageWrapper>,
        val preProcessedMessageQueue: BlockingQueue<MessageWrapper>
) {
    fun process(wrapper: MessageWrapper) {
        when (wrapper.type) {
            MessageType.CASH_IN_OUT_OPERATION.type -> cashInOutInputQueue.put(wrapper)
            MessageType.CASH_TRANSFER_OPERATION.type -> cashTransferInputQueue.put(wrapper)
            MessageType.LIMIT_ORDER_CANCEL.type,
            MessageType.OLD_LIMIT_ORDER_CANCEL.type -> limitOrderCancelInputQueue.put(wrapper)
            else -> preProcessedMessageQueue.put(wrapper)
        }
    }
}
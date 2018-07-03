package com.lykke.matching.engine.incoming

import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import java.util.concurrent.BlockingQueue

class MessageRouter(
        val cashInOutQueue: BlockingQueue<MessageWrapper>,
        val cashTransferQueue: BlockingQueue<MessageWrapper>,
        val defaultMessagesQueue: BlockingQueue<MessageWrapper>
) {
    fun process(wrapper: MessageWrapper) {
        when(wrapper.type) {
            MessageType.CASH_IN_OUT_OPERATION.type -> cashInOutQueue.put(wrapper)
            MessageType.CASH_TRANSFER_OPERATION.type -> cashTransferQueue.put(wrapper)
            else -> defaultMessagesQueue.put(wrapper)
        }
    }
}
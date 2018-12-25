package com.lykke.matching.engine.holders
import com.lykke.matching.engine.messages.MessageType
import org.springframework.stereotype.Component

@Component
class CurrentTransactionDataHolder {
    private val messageTypeThreadLocal = ThreadLocal<MessageType>()

    fun getMessageType(): MessageType? {
        return messageTypeThreadLocal.get()
    }

    fun setMessageType(messageType: MessageType) {
        messageTypeThreadLocal.set(messageType)
    }
}
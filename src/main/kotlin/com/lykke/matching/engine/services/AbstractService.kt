package com.lykke.matching.engine.services

import com.google.protobuf.MessageOrBuilder
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageWrapper

interface AbstractService<out T : MessageOrBuilder> {
    fun parseMessage(messageWrapper: MessageWrapper)
    fun processMessage(messageWrapper: MessageWrapper)
    fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus)
}
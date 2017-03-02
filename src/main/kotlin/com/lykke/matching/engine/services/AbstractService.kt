package com.lykke.matching.engine.services

import com.google.protobuf.MessageOrBuilder
import com.lykke.matching.engine.messages.MessageWrapper

interface AbstractService<T : MessageOrBuilder> {
    fun processMessage(messageWrapper: MessageWrapper)
}
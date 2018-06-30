package com.lykke.matching.engine.incoming.preprocessor

import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageWrapper

interface MessagePreprocessor {
    fun parseMessage(messageWrapper: MessageWrapper)
    fun preprocess(messageWrapper: MessageWrapper)
    fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus)
}
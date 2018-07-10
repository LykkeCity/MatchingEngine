package com.lykke.matching.engine.incoming.preprocessor

import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageWrapper

interface MessagePreprocessor {
    fun preProcess(messageWrapper: MessageWrapper)
    fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus)
}
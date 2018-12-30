package com.lykke.matching.engine.incoming.preprocessor

import com.lykke.matching.engine.messages.MessageWrapper

interface MessagePreprocessor {
    fun preProcess(messageWrapper: MessageWrapper)
}
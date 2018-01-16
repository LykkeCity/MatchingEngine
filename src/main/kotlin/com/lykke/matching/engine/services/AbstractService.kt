package com.lykke.matching.engine.services

import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageWrapper

interface AbstractService {
    fun parseMessage(messageWrapper: MessageWrapper)
    fun processMessage(messageWrapper: MessageWrapper)
    fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus)
}
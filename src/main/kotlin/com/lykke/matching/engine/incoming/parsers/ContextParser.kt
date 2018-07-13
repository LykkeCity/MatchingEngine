package com.lykke.matching.engine.incoming.parsers

import com.lykke.matching.engine.messages.MessageWrapper

interface ContextParser {
    fun parse(messageWrapper: MessageWrapper): MessageWrapper
}
package com.lykke.matching.engine.incoming.parsers

import com.lykke.matching.engine.messages.MessageWrapper

interface ContextParser<T> {
    fun parse(messageWrapper: MessageWrapper): T
}
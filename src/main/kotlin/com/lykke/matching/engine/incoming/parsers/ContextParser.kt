package com.lykke.matching.engine.incoming.parsers

import com.lykke.matching.engine.messages.MessageWrapper

interface ContextParser<out T> {
    fun parse(messageWrapper: MessageWrapper): T
}
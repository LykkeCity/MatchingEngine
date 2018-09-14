package com.lykke.matching.engine.incoming.parsers

import com.lykke.matching.engine.messages.MessageWrapper

interface ContextParser<out R> {
    fun parse(messageWrapper: MessageWrapper): R
}
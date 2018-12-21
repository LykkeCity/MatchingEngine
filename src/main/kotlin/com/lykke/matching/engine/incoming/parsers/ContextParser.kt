package com.lykke.matching.engine.incoming.parsers

import com.lykke.matching.engine.incoming.parsers.data.ParsedData
import com.lykke.matching.engine.messages.MessageWrapper

interface ContextParser<out R: ParsedData> {
    fun parse(messageWrapper: MessageWrapper): R
}
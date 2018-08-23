package com.lykke.matching.engine.logging

import com.lykke.matching.engine.daos.Message

interface LogMessageTransformer {
    fun transform(message: Any): Message
}
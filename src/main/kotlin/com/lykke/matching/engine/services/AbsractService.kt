package com.lykke.matching.engine.services

import com.google.protobuf.MessageOrBuilder

interface AbsractService<T : MessageOrBuilder> {
    fun processMessage(array: ByteArray)
}
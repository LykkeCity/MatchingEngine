package com.lykke.matching.engine.logging

import com.lykke.matching.engine.outgoing.messages.JsonSerializable

data class MessageWrapper(val message: JsonSerializable, val json: String)
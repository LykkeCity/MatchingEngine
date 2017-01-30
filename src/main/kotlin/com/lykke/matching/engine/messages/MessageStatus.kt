package com.lykke.matching.engine.messages

enum class MessageStatus(val type: Int){
    OK(0),
    LOW_BALANCE(401),
    ALREADY_PROCESSED(402),
    RUNTIME(500)
}
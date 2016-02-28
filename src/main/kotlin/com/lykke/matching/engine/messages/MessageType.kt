package com.lykke.matching.engine.messages

import java.util.HashMap

enum class MessageType (val type: Int){
    PING(1)
    ,UPDATE_BALANCE(2)
    ,LIMIT_ORDER(3)
    ,MARKET_ORDER(4)
    ;


    companion object {
        val typesMap = HashMap<Int, MessageType>()

        init {
            values().forEach {
                typesMap.put(it.type, it)
            }
        }

        fun valueOf(type: Int): MessageType? {
            return typesMap[type]
        }
    }
}
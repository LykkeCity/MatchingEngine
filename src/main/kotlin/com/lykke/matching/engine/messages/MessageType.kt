package com.lykke.matching.engine.messages

import java.util.*

enum class MessageType (val type: Byte){
    RESPONSE(0)
    ,PING(1)
    ,OLD_LIMIT_ORDER(3)
    ,OLD_MARKET_ORDER(4)
    ,OLD_LIMIT_ORDER_CANCEL(5)
    ,OLD_MULTI_LIMIT_ORDER(7)
    ,CASH_TRANSFER_OPERATION(8)
    ,CASH_IN_OUT_OPERATION(9)
    ,ORDER_BOOK_SNAPSHOT(40)
    ,LIMIT_ORDER(50)
    ,MULTI_LIMIT_ORDER(51)
    ,MARKET_ORDER(52)
    ,NEW_MARKET_ORDER(53)
    ,LIMIT_ORDER_CANCEL(55)
    ,MULTI_LIMIT_ORDER_CANCEL(57)
    ,NEW_RESPONSE(99)
    ,MARKER_ORDER_RESPONSE(100)
    ,MULTI_LIMIT_ORDER_RESPONSE(98)
    ,RESERVED_CASH_IN_OUT_OPERATION(120)
    ,LIMIT_ORDER_MASS_CANCEL(121)
    ;


    companion object {
        private val typesMap = HashMap<Byte, MessageType>()

        init {
            values().forEach {
                typesMap.put(it.type, it)
            }
        }

        fun valueOf(type: Byte): MessageType? {
            return typesMap[type]
        }
    }
}
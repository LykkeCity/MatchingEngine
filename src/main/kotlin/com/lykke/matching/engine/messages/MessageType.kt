package com.lykke.matching.engine.messages

import java.util.HashMap

enum class MessageType (val type: Byte){
    RESPONSE(0)
    ,PING(1)
    ,CASH_OPERATION(2)
    ,OLD_LIMIT_ORDER(3)
    ,OLD_MARKET_ORDER(4)
    ,OLD_LIMIT_ORDER_CANCEL(5)
    ,OLD_BALANCE_UPDATE(6)
    ,OLD_MULTI_LIMIT_ORDER(7)
    ,CASH_TRANSFER_OPERATION(8)
    ,CASH_IN_OUT_OPERATION(9)
    ,CASH_SWAP_OPERATION(10)
    ,BALANCE_UPDATE(11)
    ,BALANCE_UPDATE_SUBSCRIBE(30)
    ,BALANCE_UPDATE_NOTIFICATION(31)
    ,QUOTES_UPDATE_SUBSCRIBE(35)
    ,QUOTES_UPDATE_NOTIFICATION(36)
    ,ORDER_BOOK_SNAPSHOT(40)
    ,LIMIT_ORDER(50)
    ,MULTI_LIMIT_ORDER(51)
    ,MARKET_ORDER(52)
    ,NEW_MARKET_ORDER(53)
    ,LIMIT_ORDER_CANCEL(55)
    ,MULTI_LIMIT_ORDER_CANCEL(57)
    ,NEW_RESPONSE(99)
    ,MARKER_ORDER_RESPONSE(100)
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
package com.lykke.matching.engine.messages

import java.util.HashMap

enum class MessageType (val type: Byte){
    RESPONSE(0)
    ,PING(1)
    ,CASH_OPERATION(2)
    ,OLD_LIMIT_ORDER(3)
    ,MARKET_ORDER(4)
    ,OLD_LIMIT_ORDER_CANCEL(5)
    ,BALANCE_UPDATE(6)
    ,OLD_MULTI_LIMIT_ORDER(7)
    ,CASH_TRANSFER_OPERATION(8)
    ,CASH_IN_OUT_OPERATION(9)
    ,CASH_SWAP_OPERATION(10)
    ,WALLET_CREDENTIALS_RELOAD(20)
    ,BALANCE_UPDATE_SUBSCRIBE(30)
    ,BALANCE_UPDATE_NOTIFICATION(31)
    ,QUOTES_UPDATE_SUBSCRIBE(35)
    ,QUOTES_UPDATE_NOTIFICATION(36)
    ,ORDER_BOOK_SNAPSHOT(40)
    ,LIMIT_ORDER(50)
    ,MULTI_LIMIT_ORDER(51)
    ,LIMIT_ORDER_CANCEL(55)
    ,NEW_RESPONSE(99)
    ;


    companion object {
        val typesMap = HashMap<Byte, MessageType>()

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
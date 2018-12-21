package com.lykke.matching.engine.database.common.entity

import com.lykke.matching.engine.deduplication.ProcessedMessage
import org.springframework.util.CollectionUtils

class PersistenceData(val balancesData: BalancesData?,
                      val processedMessage: ProcessedMessage? = null,
                      val orderBooksData: OrderBooksPersistenceData?,
                      val stopOrderBooksData: OrderBooksPersistenceData?,
                      val messageSequenceNumber: Long?) {

    constructor(processedMessage: ProcessedMessage?, messageSequenceNumber: Long?) : this(null, processedMessage, null, null, messageSequenceNumber)
    constructor(processedMessage: ProcessedMessage?) : this(null, processedMessage, null, null, null)

    fun isEmpty(): Boolean {
        return isEmptyWithoutOrders() &&
                isOrdersEmpty()
    }

    fun isOrdersEmpty(): Boolean {
        return (orderBooksData == null || orderBooksData.isEmpty()) &&
                (stopOrderBooksData == null || stopOrderBooksData.isEmpty())
    }

    fun isEmptyWithoutOrders(): Boolean {
        return CollectionUtils.isEmpty(balancesData?.balances) &&
                CollectionUtils.isEmpty(balancesData?.wallets) &&
                processedMessage == null &&
                messageSequenceNumber == null
    }

    fun getSummary(): String {
        val result = ArrayList<String>()

        balancesData?.let {
            result.add("w: ${it.wallets.size}")
            result.add("b: ${it.balances.size}")
        }

        orderBooksData?.let {
            result.add("ob: ${it.orderBooks.size}")
            result.add("os: ${it.ordersToSave.size}")
            result.add("or: ${it.ordersToRemove.size}")
        }

        stopOrderBooksData?.let {
            result.add("sob: ${it.orderBooks.size}")
            result.add("sos: ${it.ordersToSave.size}")
            result.add("sor: ${it.ordersToRemove.size}")
        }

        messageSequenceNumber?.let {
            result.add("sn: $messageSequenceNumber")
        }

        return result.joinToString()
    }
}
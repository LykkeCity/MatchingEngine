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
        val result = StringBuilder()

        balancesData?.let {
            result.append("w: ${it.wallets.size}, ")
                    .append("b: ${it.balances.size}, ")
        }

        orderBooksData?.let {
            result.append("ob: ${it.orderBooks.size}, ")
                    .append("os: ${it.ordersToSave.size}, ")
                    .append("or: ${it.ordersToRemove.size}, ")
        }

        stopOrderBooksData?.let {
            result.append("sob: ${it.orderBooks.size}, ")
                    .append("sos: ${it.ordersToSave.size}, ")
                    .append("sor: ${it.ordersToRemove.size}, ")
        }

        messageSequenceNumber?.let {
            result.append("sn: $messageSequenceNumber")
        }


        return result.toString()
    }

    private fun append(builder: StringBuilder, prefix: String, obj: Any?) {
        obj?.let {
            if (builder.isNotEmpty()) {
                builder.append(", ")
            }
            builder.append(prefix).append(obj)
        }
    }
}
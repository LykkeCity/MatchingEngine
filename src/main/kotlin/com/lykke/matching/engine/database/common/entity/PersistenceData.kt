package com.lykke.matching.engine.database.common.entity

import com.lykke.matching.engine.deduplication.ProcessedMessage

class PersistenceData(val balancesData: BalancesData?,
                      val processedMessage: ProcessedMessage? = null,
                      val messageSequenceNumber: Long?) {

    constructor(processedMessage: ProcessedMessage?, messageSequenceNumber: Long?): this(null, processedMessage, messageSequenceNumber)
    constructor(processedMessage: ProcessedMessage?): this(null, processedMessage, null)

    fun details() = "w: ${balancesData?.wallets?.size}, b: ${balancesData?.balances?.size}, m: ${processedMessage?.messageId}, sn: $messageSequenceNumber"
}
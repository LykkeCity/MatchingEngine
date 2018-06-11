package com.lykke.matching.engine.database.common.entity


import com.lykke.matching.engine.deduplication.ProcessedMessage

class PersistenceData(val balancesData: BalancesData?,
                      val processedMessage: ProcessedMessage? = null) {

    constructor(processedMessage: ProcessedMessage?): this(null, processedMessage)

    fun details() = "w: ${balancesData?.wallets?.size}, b: ${balancesData?.balances?.size}, m: ${processedMessage?.messageId}"
}
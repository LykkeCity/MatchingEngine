package com.lykke.matching.engine.database.common.entity


import com.lykke.matching.engine.deduplication.ProcessedMessage

class PersistenceData(val balancesData: BalancesData?,
                      var processedMessage: ProcessedMessage? = null) {

    fun details() = "w: ${balancesData?.wallets?.size}, b: ${balancesData?.balances?.size}, m: ${processedMessage?.messageId}"

}
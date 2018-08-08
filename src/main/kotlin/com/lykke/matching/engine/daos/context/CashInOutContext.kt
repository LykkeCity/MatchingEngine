package com.lykke.matching.engine.daos.context

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.CashInOutOperation
import com.lykke.matching.engine.deduplication.ProcessedMessage

data class CashInOutContext(val messageId: String,
                       val clientId: String,
                       val processedMessage: ProcessedMessage,
                       val cashInOutOperation: CashInOutOperation,
                       val asset: Asset?)
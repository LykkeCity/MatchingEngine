package com.lykke.matching.engine.daos.context

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.CashInOutOperation
import com.lykke.matching.engine.deduplication.ProcessedMessage
import java.util.*

class CashInOutContext(val id: String,
                       val messageId: String,
                       val operationId: String,
                       val clientId: String,
                       val processedMessage: ProcessedMessage?,
                       val cashInOutOperation: CashInOutOperation,
                       val asset: Asset?,
                       val operationStartTime: Date)
package com.lykke.matching.engine.daos.context

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.daos.fee.v2.NewFeeInstruction
import com.lykke.matching.engine.deduplication.ProcessedMessage
import java.math.BigDecimal
import java.util.*

class CashInOutContext(val id: String,
                       val messageId: String,
                       val operationId: String,
                       val clientId: String,
                       val processedMessage: ProcessedMessage?,
                       val feeInstructions: List<NewFeeInstruction>,
                       val walletOperation: WalletOperation,
                       val asset: Asset,
                       val volume: BigDecimal,
                       val operationStartTime: Date)
package com.lykke.matching.engine.outgoing.messages.v2

import com.lykke.matching.engine.balance.WalletOperationsProcessor
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.daos.fee.v2.Fee
import java.util.*

class CashInOutEventData(val messageId: String,
                         val externalId: String,
                         val sequenceNumber: Long,
                         val now: Date,
                         val timestamp: Date,
                         val walletProcessor: WalletOperationsProcessor,
                         val walletOperation: WalletOperation,
                         val asset: Asset,
                         val internalFees: List<Fee>
)
package com.lykke.matching.engine.outgoing.messages

import com.lykke.matching.engine.balance.WalletOperationsProcessor
import com.lykke.matching.engine.daos.OutgoingEventData
import com.lykke.matching.engine.daos.WalletOperation
import java.util.Date

class ReservedCashInOutEventData(val sequenceNumber: Long,
                                 val messageId: String,
                                 val requestId: String,
                                 val date: Date,
                                 val clientBalanceUpdates: List<ClientBalanceUpdate>,
                                 val walletOperation: WalletOperation,
                                 @Deprecated("The field is used only in deprecated old event format sender")
                                 val walletOperationsProcessor: WalletOperationsProcessor,
                                 @Deprecated("The field is used only in deprecated old event format sender")
                                 val accuracy: Int) : OutgoingEventData
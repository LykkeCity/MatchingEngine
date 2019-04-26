package com.lykke.matching.engine.outgoing.messages

import com.lykke.matching.engine.daos.OutgoingEventData
import com.lykke.matching.engine.daos.TransferOperation
import com.lykke.matching.engine.daos.fee.v2.Fee
import java.util.*

class CashTransferEventData(val messageId: String,
                            val clientBalanceUpdates: List<ClientBalanceUpdate>,
                            val fees: List<Fee>,
                            val transferOperation: TransferOperation,
                            val sequenceNumber: Long,
                            val now: Date): OutgoingEventData
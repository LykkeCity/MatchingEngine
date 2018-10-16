package com.lykke.matching.engine.outgoing.messages

import com.lykke.matching.engine.daos.fee.v2.Fee
import java.util.Date

class CashOperation(val id: String,
                    val clientId: String,
                    val dateTime: Date,
                    val volume: String,
                    val asset: String,
                    val messageId: String,
                    val fees: List<Fee>?)
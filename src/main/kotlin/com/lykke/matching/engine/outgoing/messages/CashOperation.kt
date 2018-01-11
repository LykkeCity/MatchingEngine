package com.lykke.matching.engine.outgoing.messages

import com.lykke.matching.engine.daos.FeeInstruction
import com.lykke.matching.engine.daos.FeeTransfer
import java.util.Date

class CashOperation(val id: String,
                    val clientId: String,
                    val dateTime: Date,
                    val volume: String,
                    val asset: String,
                    val feeInstructions: List<FeeInstruction>?,
                    val feeTransfers: List<FeeTransfer>?): JsonSerializable()
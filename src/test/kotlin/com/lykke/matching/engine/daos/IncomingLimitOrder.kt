package com.lykke.matching.engine.daos

import com.lykke.matching.engine.daos.fee.NewLimitOrderFeeInstruction
import java.util.UUID

data class IncomingLimitOrder(val volume: Double,
                              val price: Double,
                              val uid: String? = UUID.randomUUID().toString(),
                              val feeInstruction: LimitOrderFeeInstruction? = null,
                              val feeInstructions: List<NewLimitOrderFeeInstruction> = emptyList(),
                              val oldUid: String? = null)
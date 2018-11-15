package com.lykke.matching.engine.daos

import com.lykke.matching.engine.daos.fee.v2.NewLimitOrderFeeInstruction
import com.lykke.matching.engine.daos.order.LimitOrderType
import com.lykke.matching.engine.daos.v2.LimitOrderFeeInstruction
import java.util.UUID

data class IncomingLimitOrder(val volume: Double,
                              val price: Double? = null,
                              val uid: String? = UUID.randomUUID().toString(),
                              val feeInstruction: LimitOrderFeeInstruction? = null,
                              val feeInstructions: List<NewLimitOrderFeeInstruction> = emptyList(),
                              val oldUid: String? = null,
                              val type: LimitOrderType? = null,
                              val lowerLimitPrice: Double? = null,
                              val lowerPrice: Double? = null,
                              val upperLimitPrice: Double? = null,
                              val upperPrice: Double? = null)
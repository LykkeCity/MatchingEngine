package com.lykke.matching.engine.order.process.common

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import org.apache.log4j.Logger
import java.util.Date

class CancelRequest(val limitOrders: Collection<LimitOrder>,
                    val stopLimitOrders: Collection<LimitOrder>,
                    val assetPairId: String?,
                    val messageId: String,
                    val requestId: String,
                    val messageType: MessageType,
                    val date: Date,
                    val processedMessage: ProcessedMessage?,
                    val messageWrapper: MessageWrapper?,
                    val logger: Logger)
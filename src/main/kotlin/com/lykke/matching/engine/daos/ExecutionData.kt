package com.lykke.matching.engine.daos

import com.lykke.matching.engine.order.SequenceNumbersWrapper
import com.lykke.matching.engine.order.transaction.ExecutionContext

class ExecutionData(val executionContext: ExecutionContext,
                    val sequenceNumbers: SequenceNumbersWrapper)
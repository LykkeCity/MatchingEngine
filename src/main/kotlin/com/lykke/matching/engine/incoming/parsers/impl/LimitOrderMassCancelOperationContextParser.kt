package com.lykke.matching.engine.incoming.parsers.impl

import com.lykke.matching.engine.incoming.data.LimitOrderMassCancelOperationParsedData
import com.lykke.matching.engine.incoming.parsers.ContextParser
import com.lykke.matching.engine.messages.MessageWrapper

class LimitOrderMassCancelOperationContextParser: ContextParser<LimitOrderMassCancelOperationParsedData> {
    override fun parse(messageWrapper: MessageWrapper): LimitOrderMassCancelOperationParsedData {
        return LimitOrderMassCancelOperationParsedData(messageWrapper)
    }
}
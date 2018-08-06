package com.lykke.matching.engine.incoming.parsers.impl

import com.lykke.matching.engine.incoming.data.LimitOrderCancelOperationParsedData
import com.lykke.matching.engine.incoming.parsers.ContextParser
import com.lykke.matching.engine.messages.MessageWrapper

class LimitOrderCancelOperationContextParser: ContextParser<LimitOrderCancelOperationParsedData> {
    override fun parse(messageWrapper: MessageWrapper): LimitOrderCancelOperationParsedData {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}
package com.lykke.matching.engine.incoming.parsers.data

import com.lykke.matching.engine.messages.MessageWrapper

class SingleLimitOrderParsedData(messageWrapper: MessageWrapper, val inputAssetPairId: String): ParsedData(messageWrapper)
package com.lykke.matching.engine.incoming.parsers.data

import com.lykke.matching.engine.messages.MessageWrapper

class CashInOutParsedData(messageWrapper: MessageWrapper,
                          val assetId: String): ParsedData(messageWrapper)
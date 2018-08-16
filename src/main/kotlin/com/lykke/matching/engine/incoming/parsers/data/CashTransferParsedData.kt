package com.lykke.matching.engine.incoming.parsers.data

import com.lykke.matching.engine.daos.fee.v2.NewFeeInstruction
import com.lykke.matching.engine.daos.v2.FeeInstruction
import com.lykke.matching.engine.messages.MessageWrapper

class CashTransferParsedData(val messageWrapper: MessageWrapper,
                             val assetId: String,
                             val feeInstruction: FeeInstruction?,
                             val feeInstructions: List<NewFeeInstruction>)
package com.lykke.matching.engine.services.validators

import com.lykke.matching.engine.daos.FeeInstruction
import com.lykke.matching.engine.daos.fee.NewFeeInstruction
import com.lykke.matching.engine.messages.ProtocolMessages

interface CashTransferOperationValidator {
    fun performValidation(cashTransferOperation: ProtocolMessages.CashTransferOperation,
                          operationId: String, feeInstructions: List<NewFeeInstruction>,
                          feeInstruction: FeeInstruction?)
}
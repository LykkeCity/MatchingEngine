package com.lykke.matching.engine.services.validators

import com.lykke.matching.engine.messages.ProtocolMessages

interface BalanceUpdateValidator {
    fun performValidation( message: ProtocolMessages.BalanceUpdate)
}
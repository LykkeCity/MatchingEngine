package com.lykke.matching.engine.daos

import com.lykke.matching.engine.messages.MessageType
import java.io.Serializable

data class DisableFunctionalityRule(val clientId: String?,
                               val assetId: String?,
                               val assetPairId: String?,
                               val messageType: MessageType?): Serializable {
    fun isEmpty(): Boolean {
        return clientId == null
                && assetId == null
                && assetPairId == null
                && messageType == null
    }
}
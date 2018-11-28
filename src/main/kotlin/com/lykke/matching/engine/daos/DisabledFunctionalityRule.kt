package com.lykke.matching.engine.daos

import com.lykke.matching.engine.messages.MessageType
import java.io.Serializable

data class DisabledFunctionalityRule(
        val assetId: String?,
        val assetPairId: String?,
        val messageType: MessageType?) : Serializable {
    fun isEmpty(): Boolean {
        return assetId == null
                && assetPairId == null
                && messageType == null
    }
}
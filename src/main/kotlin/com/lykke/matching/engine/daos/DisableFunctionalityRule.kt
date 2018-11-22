package com.lykke.matching.engine.daos

import com.lykke.matching.engine.messages.MessageType
import java.io.Serializable

data class DisableFunctionalityRule(
        val asset: Asset?,
        val assetPair: AssetPair?,
        val messageType: MessageType?) : Serializable {
    fun isEmpty(): Boolean {
        return asset == null
                && assetPair == null
                && messageType == null
    }
}
package com.lykke.matching.engine.daos

data class DisabledFunctionalityRule(
        val assetId: String?,
        val assetPairId: String?,
        val operationType: OperationType?) {
    fun isEmpty(): Boolean {
        return assetId == null
                && assetPairId == null
                && operationType == null
    }
}
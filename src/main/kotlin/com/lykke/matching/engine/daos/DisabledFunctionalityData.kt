package com.lykke.matching.engine.daos

class DisabledFunctionalityData(
        val disabledAssetPairIds: Set<String> = HashSet(),
        val disabledOperations: Set<OperationType> = HashSet(),
        val disabledAssetIds: Set<String> = HashSet(),
        val disabledOperationsByAsset: Map<String, MutableSet<OperationType>> = HashMap()
)
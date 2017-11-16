package com.lykke.matching.engine.daos

enum class FeeSizeType constructor(val externalId: Int) {

    PERCENTAGE(0),
    ABSOLUTE(1);

    companion object {
        fun getByExternalId(externalId: Int): FeeSizeType {
            FeeSizeType.values()
                    .filter { it.externalId == externalId }
                    .forEach { return it }
            throw IllegalArgumentException("FeeTypeSize (externalId=$externalId) is not found")
        }
    }

}
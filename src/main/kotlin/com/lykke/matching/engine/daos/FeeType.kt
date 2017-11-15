package com.lykke.matching.engine.daos

enum class FeeType constructor(val externalId: Int) {

    NO_FEE(0),
    CLIENT_FEE(1),
    EXTERNAL_FEE(2);

    companion object {
        fun getByExternalId(externalId: Int): FeeType {
            FeeType.values()
                    .filter { it.externalId == externalId }
                    .forEach { return it }
            throw IllegalArgumentException("FeeType (externalId=$externalId) is not found")
        }
    }

}
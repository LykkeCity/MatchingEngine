package com.lykke.matching.engine.utils

fun <T> plus(collection1: Collection<T>, collection2: Collection<T>): Collection<T> {
    return when {
        collection1.isEmpty() -> collection2
        collection2.isEmpty() -> collection1
        else -> collection1 + collection2
    }
}
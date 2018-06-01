package com.lykke.matching.engine.daos

class CopyWrapper<out T : Copyable>(value: T, isNew: Boolean = false) {

    val origin: T?
    val copy: T

    fun applyToOrigin(): T {
        if (origin == null) {
            return copy
        }
        copy.applyToOrigin(origin)
        return origin
    }

    init {
        if (isNew) {
            origin = null
            copy = value
        } else {
            origin = value
            @Suppress("unchecked_cast")
            copy = value.copy() as T
        }
    }
}

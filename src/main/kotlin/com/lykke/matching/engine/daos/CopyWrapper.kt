package com.lykke.matching.engine.daos

import com.lykke.matching.engine.updater.Copyable

class CopyWrapper<out T : Copyable>(value: T, isNew: Boolean = false) {

    private val origin: T?
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
            copy = value.copy() as T
        }
    }
}

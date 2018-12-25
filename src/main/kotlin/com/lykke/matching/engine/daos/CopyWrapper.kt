package com.lykke.matching.engine.daos

class CopyWrapper<out T : Copyable> {

    val origin: T?
    val copy: T

    constructor(value: T, isNew: Boolean = false) {
        if (isNew) {
            origin = null
            copy = value
        } else {
            origin = value
            @Suppress("unchecked_cast")
            copy = value.copy() as T
        }
    }

    constructor(origin: T?, copy: T) {
        this.origin = origin
        this.copy = copy
    }

    fun applyToOrigin(): T {
        if (origin == null) {
            return copy
        }
        copy.applyToOrigin(origin)
        return origin
    }
}

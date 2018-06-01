package com.lykke.matching.engine.database.redis

class EmptyJedisHolder: JedisHolder {
    override fun ok() = true
}
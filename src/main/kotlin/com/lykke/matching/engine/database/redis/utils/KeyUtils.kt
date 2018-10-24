package com.lykke.matching.engine.database.redis.utils

import redis.clients.jedis.Transaction

class KeyUtils {
    companion object {
        private val LUA_REMOVE_KEYS_BY_PATTERN_SCRIPT = """local keys = redis.call('keys', %s)
            | for i,k in ipairs(keys) do
            |   redis.call('del', k)
            | end
        """.trimMargin()

        fun removeAllKeysByPattern(transaction: Transaction, pattern: String) {
            transaction.eval(LUA_REMOVE_KEYS_BY_PATTERN_SCRIPT.format(pattern))
        }
    }
}
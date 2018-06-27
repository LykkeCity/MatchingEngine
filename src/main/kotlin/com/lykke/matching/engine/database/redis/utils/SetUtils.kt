package com.lykke.matching.engine.database.redis.utils

import redis.clients.jedis.Transaction

class SetUtils {
    companion object {
        private val LUA_ATOMIC_SAVE_SET_EXPIRE_SCRIPT = """local firstSave = redis.call('exists', KEYS[1])
            |redis.call('sadd', KEYS[1], ARGV[1])
            |if firstSave == 0
            |then redis.call('expire', KEYS[1], ARGV[2])
            |end""".trimMargin()

        fun performAtomicSaveSetExpire(transaction: Transaction, key: String, value: String, timeToLive: Int) {
            transaction.eval(LUA_ATOMIC_SAVE_SET_EXPIRE_SCRIPT,
                    1,
                    key,
                    value,
                    timeToLive.toString())
        }
    }
}
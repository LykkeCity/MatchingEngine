package com.lykke.matching.engine.database.redis.utils

import redis.clients.jedis.Transaction
import java.lang.StringBuilder

class BulkUtils {
    companion object {
        private const val BULK_INSERT_LUA_SCRIPT_EXPIRE = "redis.call('set', '%s', ARGV[%d], 'ex', %d) "
        private const val BULK_INSERT_LUA_SCRIPT = "redis.call('set', '%s', ARGV[%d]) "

        fun bulkInsert(transaction: Transaction, valuesToKeys: Map<String, ByteArray>, expire: Int? = null) {
            val keys = ArrayList<String>()
            val values = ArrayList<ByteArray>()

            valuesToKeys.forEach { key, value ->
                keys.add(key)
                values.add(value)
            }

            transaction.eval(getLuaScript(keys, expire).toByteArray(), ArrayList<ByteArray>(), values)
        }

        private fun getLuaScript(keys: List<String>, expire: Int?): String {
            val luaScript = StringBuilder()

            val scriptFormat =  if (expire != null) {
                BULK_INSERT_LUA_SCRIPT_EXPIRE
            } else {
                BULK_INSERT_LUA_SCRIPT
            }

            keys.forEachIndexed { index, element ->
                luaScript.append(scriptFormat.format(element, index + 1, expire))
            }

            return luaScript.toString()
        }
    }
}
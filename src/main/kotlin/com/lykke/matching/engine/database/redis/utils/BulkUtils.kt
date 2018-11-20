package com.lykke.matching.engine.database.redis.utils

import redis.clients.jedis.Transaction
import java.lang.StringBuilder

class BulkUtils {
    companion object {
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
            return if (expire != null) {
                getLuaBulkInsertExpireScript(keys, expire)
            } else {
                getLuaBulkInsertScript(keys)
            }
        }

        private fun getLuaBulkInsertExpireScript(keys: List<String>, expire: Int): String {
            val result = StringBuilder()

            keys.forEachIndexed { index, key ->
                result.append("redis.call('set', '$key', ARGV[${index + 1}], 'ex', $expire) ")
            }

            return result.toString()
        }

        private fun getLuaBulkInsertScript(keys: List<String>): String {
            val result = StringBuilder()

            keys.forEachIndexed { index, key ->
                result.append("redis.call('set', '$key', ARGV[${index + 1}]) ")
            }

            return result.toString()
        }
    }
}
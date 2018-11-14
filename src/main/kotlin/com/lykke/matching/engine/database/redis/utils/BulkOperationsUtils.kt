package com.lykke.matching.engine.database.redis.utils

import org.springframework.util.CollectionUtils
import redis.clients.jedis.Transaction

class BulkOperationsUtils {
    companion object {
        private val LUA_SET_FORMAT = " redis.call('set', '%s', '%s') "
        private val LUA_SET_FORMAT_TTL = " redis.call('set', '%s', '%s', 'px', '%s') "

        fun insertBulk(transaction: Transaction, entitiesByKey: Map<String, String>, ttl: Long? = null) {
            if (CollectionUtils.isEmpty(entitiesByKey)) {
                return
            }

            val script = if (ttl != null) {
                getTTLBulkSetLuaScript(entitiesByKey, ttl)
            } else {
                getBulkSetSuaScript(entitiesByKey)
            }

            transaction.eval(script)
        }

        private fun getBulkSetSuaScript(entitiesByKey: Map<String, String>): String {
            val luaScript = StringBuilder()
            entitiesByKey.forEach { key, value -> luaScript.append(LUA_SET_FORMAT.format(key, value)) }

            return luaScript.toString()
        }


        private fun getTTLBulkSetLuaScript(entitiesByKey: Map<String, String>, ttl: Long): String {
            val luaScript = StringBuilder()
            entitiesByKey.forEach { key, value -> luaScript.append(LUA_SET_FORMAT_TTL.format(key, value, ttl.toString())) }

            return luaScript.toString()
        }
    }
}
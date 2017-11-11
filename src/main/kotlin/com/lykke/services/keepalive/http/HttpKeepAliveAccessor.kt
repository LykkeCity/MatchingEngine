package com.lykke.services.keepalive.http

import com.google.gson.Gson
import com.lykke.matching.engine.logging.ThrottlingLogger
import com.lykke.services.keepalive.KeepAliveAccessor
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.log4j.Logger
import java.util.Date

class HttpKeepAliveAccessor(
        private val path: String): KeepAliveAccessor {

    companion object {
        val LOGGER = ThrottlingLogger.getLogger(HttpKeepAliveAccessor::class.java.name)
    }

    private val gson = Gson()

    override fun updateKeepAlive(date: Date, service: String, version: String) {
        while (!sendHttpRequest(KeepAlive(service, version))) {}
    }

    private fun sendHttpRequest(keepAlive: KeepAlive): Boolean {
        try {
            val requestConfig = RequestConfig.custom().setConnectTimeout(10 * 1000).build()
            val httpClient = HttpClientBuilder.create().setDefaultRequestConfig(requestConfig).build()
            val request = HttpPost(path)
            val params = StringEntity(gson.toJson(keepAlive))
            request.addHeader("content-type", "application/json")
            request.entity = params
            httpClient.execute(request)
        } catch (e : Exception) {
            LOGGER.error("Unable to write log to http: ${e.message}", e)
            return false
        }
        return true
    }
}

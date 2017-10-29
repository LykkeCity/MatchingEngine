package com.lykke.matching.engine.logging

import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.log4j.Logger
import java.util.concurrent.BlockingQueue

class HttpLogger(private val path: String, val queue: BlockingQueue<LoggableObject>) : Thread() {
    companion object {
        val LOGGER = Logger.getLogger(HttpLogger::class.java.name)
    }

    override fun run() {
        while (true) {
            val obj = queue.take()
            sendHttpRequest(obj)
        }
    }

    private fun sendHttpRequest(obj: LoggableObject) {
        while (true) {
            try {
                val requestConfig = RequestConfig.custom().setConnectTimeout(10 * 1000).build()
                val httpClient = HttpClientBuilder.create().setDefaultRequestConfig(requestConfig).build()
                val request = HttpPost(path)
                val params = StringEntity(obj.getJson())
                request.addHeader("content-type", "application/json")
                request.entity = params
                httpClient.execute(request)
                return
            } catch (e : Exception) {
                LOGGER.error("Unable to write log to http: ${e.message}", e)
            }
        }
    }
}
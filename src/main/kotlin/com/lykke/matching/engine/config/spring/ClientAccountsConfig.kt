package com.lykke.matching.engine.config.spring

import com.lykke.client.accounts.ClientAccountCacheFactory
import com.lykke.client.accounts.ClientAccountsCache
import com.lykke.matching.engine.LOGGER
import com.lykke.matching.engine.utils.config.Config
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.lang.IllegalArgumentException
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import javax.annotation.PreDestroy
import com.lykke.client.accounts.config.Config as ClientAccountsLibConfig
import com.lykke.client.accounts.config.RabbitMqConfig as ClientAccountsRmqConfig
import com.lykke.client.accounts.config.HttpConfig as ClientAccountsHttpConfig

@Configuration
open class ClientAccountsConfig {

    @Autowired
    @InputQueue
    private lateinit var config: Config

    @Bean
    open fun clientAccountEventsQueue(): BlockingQueue<ByteArray> {
        return LinkedBlockingQueue<ByteArray>()
    }

    @Bean
    open fun clientAccountsCache(): ClientAccountsCache {
        val clientAccountsConfig = config.clientAccountsLibConfig

        if (clientAccountsConfig.rmqConfig.queueName == null) {
            throw IllegalArgumentException("Can not create client accounts cache - RMQ queue name should be provided")
        }

        with(clientAccountsConfig) {
            return ClientAccountCacheFactory.get(ClientAccountsLibConfig(ClientAccountsRmqConfig(uri = rmqConfig.uri,
                    exchange = rmqConfig.exchange,
                    queueName = rmqConfig.queueName!!,
                    routingKey = rmqConfig.routingKey,
                    queue = clientAccountEventsQueue()),
                    ClientAccountsHttpConfig(httpConfig.baseUrl,
                            httpConfig.timeout)))
        }
    }

    @PreDestroy
    fun destroy() {
        try {
            ClientAccountCacheFactory.shutdownAll()
        } catch (e: Exception) {
            LOGGER.error("Error occurred on client accounts cache shutdown", e)
        }
    }
}
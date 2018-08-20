package com.lykke.matching.engine.web.config

import com.lykke.matching.engine.utils.config.Config
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.web.server.WebServerFactoryCustomizer
import org.springframework.boot.web.servlet.server.ConfigurableServletWebServerFactory
import org.springframework.stereotype.Component

@Component
class CustomContainer : WebServerFactoryCustomizer<ConfigurableServletWebServerFactory> {
    @Autowired
    private lateinit var config: Config

    override fun customize(factory: ConfigurableServletWebServerFactory) {
        factory.setPort(config.me.httpApiPort)
    }
}
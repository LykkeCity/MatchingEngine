package com.lykke.matching.engine.config.spring

import com.lykke.matching.engine.utils.config.Config
import org.apache.catalina.connector.Connector
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
open class WebConfig  {
    @Autowired
    private lateinit var config: Config

    @Bean
    open fun tomcatServletWebServerFactory(): TomcatServletWebServerFactory {
        val tomcat = TomcatServletWebServerFactory()

        tomcat.addAdditionalTomcatConnectors(getConnector(config.me.httpOrderBookPort))
        tomcat.addAdditionalTomcatConnectors(getConnector(config.me.httpApiPort))

        return tomcat
    }

    private fun getConnector(port: Int): Connector {
        val connector = Connector("org.apache.coyote.http11.Http11NioProtocol")
        connector.scheme = "http"
        connector.port = Integer.valueOf(port)
        return connector
    }
}
package com.lykke.matching.engine.config.spring

import com.google.gson.*
import com.lykke.matching.engine.utils.config.Config
import org.apache.catalina.connector.Connector
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.http.converter.json.GsonHttpMessageConverter
import springfox.documentation.spring.web.json.Json
import java.lang.reflect.Type

internal class SpringfoxJsonToGsonAdapter : JsonSerializer<Json> {

    override fun serialize(json: Json, type: Type, context: JsonSerializationContext): JsonElement
            = JsonParser().parse(json.value())
}

@Configuration
open class WebConfig  {
    private companion object {
        val CONNECTOR_PROTOCOL = "org.apache.coyote.http11.Http11NioProtocol"
        val SCHEMA = "http"
    }

    @Autowired
    private lateinit var config: Config

    @Bean
    open fun tomcatServletWebServerFactory(): TomcatServletWebServerFactory {
        val tomcat = TomcatServletWebServerFactory()

        tomcat.addAdditionalTomcatConnectors(getConnector(config.me.httpApiPort))

        return tomcat
    }

    @Bean
    open fun gsonHttpMessageConverter(): GsonHttpMessageConverter {
        val converter = GsonHttpMessageConverter()
        converter.gson = gson()
        return converter
    }

    private fun gson(): Gson = GsonBuilder()
            .registerTypeAdapter(Json::class.java, SpringfoxJsonToGsonAdapter())
            .create()

    private fun getConnector(port: Int): Connector {
        val connector = Connector(CONNECTOR_PROTOCOL)
        connector.scheme = SCHEMA
        connector.port = Integer.valueOf(port)
        return connector
    }
}
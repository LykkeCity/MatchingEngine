package com.lykke.matching.engine.config.spring

import com.google.gson.Gson
import com.lykke.matching.engine.daos.setting.AvailableSettingGroup
import com.lykke.matching.engine.services.validators.settings.SettingValidator
import com.lykke.matching.engine.utils.config.Config
import org.apache.catalina.connector.Connector
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.http.converter.json.GsonHttpMessageConverter
import org.springframework.web.servlet.config.annotation.ViewControllerRegistry
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer
import springfox.documentation.swagger.web.UiConfiguration
import springfox.documentation.swagger.web.UiConfigurationBuilder

@Configuration
open class WebConfig  {
    private companion object {
        val CONNECTOR_PROTOCOL = "org.apache.coyote.http11.Http11NioProtocol"
        val SCHEMA = "http"
        val ROOT_SWAGGER_PAGE_URL = "/swagger-ui.html#/"
    }

    @Autowired
    private lateinit var config: Config

    @Bean
    open fun tomcatServletWebServerFactory(): TomcatServletWebServerFactory {
        val tomcat = TomcatServletWebServerFactory(config.me.httpApiPort)

        tomcat.addAdditionalTomcatConnectors(getConnector(config.me.httpOrderBookPort))

        return tomcat
    }

    @Bean
    open fun webMvcConfigurer() = object : WebMvcConfigurer {
        override fun addViewControllers(registry: ViewControllerRegistry?) {
            registry!!.addRedirectViewController("/", ROOT_SWAGGER_PAGE_URL)
        }
    }

    @Bean
    open fun gsonHttpMessageConverter(gson: Gson): GsonHttpMessageConverter {
        val converter = GsonHttpMessageConverter()
        converter.gson = gson
        return converter
    }

    @Bean
    open fun uiConfig(): UiConfiguration {
        return UiConfigurationBuilder.builder()
                .displayRequestDuration(true)
                .validatorUrl(null)
                .build()
    }

    @Bean
    open fun settingValidators(settingValidators: List<SettingValidator>): Map<AvailableSettingGroup, List<SettingValidator>> {
        return settingValidators.groupBy { it.getSettingGroup() }
    }

    private fun getConnector(port: Int): Connector {
        val connector = Connector(CONNECTOR_PROTOCOL)
        connector.scheme = SCHEMA
        connector.port = Integer.valueOf(port)
        return connector
    }
}
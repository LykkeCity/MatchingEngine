package com.lykke.matching.engine.config.spring

import com.lykke.utils.AppVersion
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import springfox.documentation.builders.PathSelectors
import springfox.documentation.builders.RequestHandlerSelectors
import springfox.documentation.spi.DocumentationType
import springfox.documentation.spring.web.plugins.Docket
import springfox.documentation.swagger2.annotations.EnableSwagger2
import springfox.documentation.service.ApiInfo

@Configuration
@EnableSwagger2
open class SwaggerConfig {
    @Bean
    open fun apiDocket(): Docket {
        return Docket(DocumentationType.SWAGGER_2)
                .apiInfo(ApiInfo(
                        "Matching Engine",
                        "Lykke Matching engine",
                        AppVersion.VERSION,
                        "",
                        null,
                        "",
                        "",
                        emptyList()
                ))
                .select()
                .apis(RequestHandlerSelectors.basePackage("com.lykke.matching.engine.web.controllers"))
                .paths(PathSelectors.any())
                .build()

    }
}
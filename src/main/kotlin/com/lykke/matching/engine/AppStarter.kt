package com.lykke.matching.engine

import org.apache.log4j.Logger
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.ConfigurableApplicationContext
import org.springframework.core.env.SimpleCommandLinePropertySource

val LOGGER = Logger.getLogger("AppStarter")

@SpringBootApplication
open class AppStarter

fun main(args: Array<String>) {
    val context = SpringApplication.run(AppStarter::class.java, *args)

    if (!context.environment.acceptsProfiles("dev")) {
        if (args.isEmpty()) {
            LOGGER.error("Not enough args. Usage: httpConfigString")
            return
        }
    }

    addCommandLinePropertySource(args, context)

    context.getBean(Application::class.java).run()
}

private fun addCommandLinePropertySource(args: Array<String>, context: ConfigurableApplicationContext) {
    val commandLineArguments = SimpleCommandLinePropertySource(*args)
    context
            .environment
            .propertySources
            .addFirst(commandLineArguments)
}



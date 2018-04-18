package com.lykke.matching.engine

import com.lykke.matching.engine.utils.config.Config
import com.lykke.utils.AppVersion
import com.lykke.utils.logging.MetricsLogger
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
    val spotName = context.getBean(Config::class.java).me.name
    Runtime.getRuntime().addShutdownHook(ShutdownHook(spotName))
    addCommandLinePropertySource(args, context)
    try {
        context.getBean(Application::class.java).run()
    } catch (e: Exception) {
        System.exit(1)
    }
}

private fun addCommandLinePropertySource(args: Array<String>, context: ConfigurableApplicationContext) {
    val commandLineArguments = SimpleCommandLinePropertySource(*args)
    context
            .environment
            .propertySources
            .addFirst(commandLineArguments)
}

internal class ShutdownHook(private val spotName: String) : Thread() {
    init {
        this.name = "ShutdownHook"
    }

    override fun run() {
        LOGGER.info("Stopping application")
        MetricsLogger.logWarning("Spot.$spotName ${AppVersion.VERSION} : Stopped :")
    }
}



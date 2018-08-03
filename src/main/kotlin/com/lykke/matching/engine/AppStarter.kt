package com.lykke.matching.engine

import com.lykke.matching.engine.utils.config.Config
import com.lykke.utils.AppVersion
import com.lykke.utils.logging.MetricsLogger
import org.apache.log4j.Logger
import org.apache.log4j.net.SocketServer
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.builder.SpringApplicationBuilder
import org.springframework.context.ConfigurableApplicationContext
import org.springframework.core.env.SimpleCommandLinePropertySource

@SpringBootApplication
open class AppStarter

val LOGGER =  Logger.getLogger("AppStarter")

fun main(args: Array<String>) {
    try {
        val context = SpringApplicationBuilder(AppStarter::class.java)
                .initializers(AppStatusStarter())
                .run(*args)
        val spotName = context.getBean(Config::class.java).me.name
        Runtime.getRuntime().addShutdownHook(ShutdownHook(spotName))
        addCommandLinePropertySource(args, context)
        context.getBean(Application::class.java).run()
    } catch (e: Exception) {
        LOGGER.error(e.message ?: "Unable to start app", e)
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



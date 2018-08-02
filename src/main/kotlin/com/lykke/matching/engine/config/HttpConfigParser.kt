package com.lykke.matching.engine.config

import com.google.gson.FieldNamingPolicy.UPPER_CAMEL_CASE
import com.google.gson.GsonBuilder
import com.lykke.matching.engine.utils.config.Config
import org.apache.log4j.Logger
import org.springframework.beans.factory.FactoryBean
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.context.annotation.Profile
import org.springframework.core.env.Environment
import org.springframework.stereotype.Component
import java.io.BufferedReader
import java.io.InputStreamReader
import java.net.URL
import javax.annotation.PostConstruct
import javax.naming.ConfigurationException


@Component("Config")
@Profile("default", "!local_config")
class HttpConfigParser : FactoryBean<Config> {

    @Autowired
    private lateinit var enviroment: Environment

    @Autowired
    @Qualifier("appStarterLogger")
    private lateinit var LOGGER: Logger

    private lateinit var config: Config

    override fun getObjectType(): Class<*> {
        return Config::class.java
    }

    override fun getObject(): Config {
        return config
    }

    @PostConstruct
    fun init() {
        val commangLineArgs = enviroment.getProperty("nonOptionArgs", Array<String>::class.java)

        if (commangLineArgs == null) {
            LOGGER.error("Not enough args. Usage: httpConfigString")
            throw IllegalArgumentException("Not enough args. Usage: httpConfigString")
        }

        config = initConfig(commangLineArgs[0])
    }

    fun initConfig(httpString: String): Config {
        val cfgUrl = URL(httpString)
        val connection = cfgUrl.openConnection()
        val inputStream = BufferedReader(InputStreamReader(connection.inputStream))

        try {
            val response = StringBuilder()
            var inputLine = inputStream.readLine()

            while (inputLine != null) {
                response.append(inputLine)
                inputLine = inputStream.readLine()
            }

            inputStream.close()

            val gson = GsonBuilder().setFieldNamingPolicy(UPPER_CAMEL_CASE).create()
            return gson.fromJson(response.toString(), Config::class.java)
        } catch (e: Exception) {
            throw ConfigurationException("Unable to read config from $httpString: ${e.message}")
        } finally {
            inputStream.close()
        }
    }
}
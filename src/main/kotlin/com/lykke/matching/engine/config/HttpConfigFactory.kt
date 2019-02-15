package com.lykke.matching.engine.config

import com.google.gson.FieldNamingPolicy.UPPER_CAMEL_CASE
import com.google.gson.GsonBuilder
import com.lykke.matching.engine.utils.config.Config
import org.apache.log4j.Logger
import org.springframework.core.env.Environment
import java.io.BufferedReader
import java.io.InputStreamReader
import java.net.URL
import javax.naming.ConfigurationException

class HttpConfigFactory {

    companion object {
        private val LOGGER: Logger = Logger.getLogger("AppStarter")

        fun getConfig(environment: Environment): Config {
            val commangLineArgs = environment.getProperty("nonOptionArgs", Array<String>::class.java)

            if (commangLineArgs == null) {
                LOGGER.error("Not enough args. Usage: httpConfigString")
                throw IllegalArgumentException("Not enough args. Usage: httpConfigString")
            }

            return downloadConfig(commangLineArgs[0])
        }

        private fun downloadConfig(httpString: String): Config {
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
}
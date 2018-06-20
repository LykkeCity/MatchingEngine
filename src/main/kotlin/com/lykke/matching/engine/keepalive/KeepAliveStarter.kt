package com.lykke.matching.engine.keepalive

import com.lykke.matching.engine.database.redis.JedisHolder
import com.lykke.matching.engine.utils.config.Config
import com.lykke.matching.engine.utils.monitoring.GeneralHealthMonitor
import com.lykke.utils.AppVersion
import com.lykke.utils.keepalive.http.KeepAliveStarter
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.ApplicationContext
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component
import javax.annotation.PostConstruct

@Component
@Profile("default")
class KeepAliveStarter @Autowired constructor(private val config: Config,
                                              private val jedisHolder: JedisHolder,
                                              private val applicationContext: ApplicationContext) {
    @PostConstruct
    private fun start() {
        val generalHealthMonitor = GeneralHealthMonitor(listOf(jedisHolder))
        KeepAliveStarter.start(config.me.keepAlive, MeIsAliveResponseGetter(generalHealthMonitor, applicationContext), AppVersion.VERSION)
    }
}
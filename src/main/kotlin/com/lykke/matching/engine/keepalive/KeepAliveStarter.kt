package com.lykke.matching.engine.keepalive

import com.lykke.matching.engine.utils.config.Config
import com.lykke.utils.AppVersion
import com.lykke.utils.keepalive.http.KeepAliveStarter
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component
import javax.annotation.PostConstruct

@Component
@Profile("default")
class KeepAliveStarter @Autowired constructor(private val meIsAliveResponseGetter: MeIsAliveResponseGetter,
                                              private val config: Config) {
    @PostConstruct
    private fun start() {
        KeepAliveStarter.start(config.me.keepAlive, meIsAliveResponseGetter, AppVersion.VERSION)
    }
}
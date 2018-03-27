package com.lykke.matching.engine.database

import org.springframework.context.annotation.Primary
import org.springframework.stereotype.Component

@Component
@Primary
class TestSettingsDatabaseAccessor : SettingsDatabaseAccessor {
    override fun loadDisabledPairs(): Set<String> {
        return emptySet()
    }
}
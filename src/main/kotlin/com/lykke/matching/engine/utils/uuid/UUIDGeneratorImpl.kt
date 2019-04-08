package com.lykke.matching.engine.utils.uuid

import org.springframework.stereotype.Component
import java.util.UUID

@Component
class UUIDGeneratorImpl : UUIDGenerator {
    override fun generate(): String {
        return UUID.randomUUID().toString()
    }
}
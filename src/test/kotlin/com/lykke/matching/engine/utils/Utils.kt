package com.lykke.matching.engine.utils

import com.lykke.matching.engine.daos.setting.Setting
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.order.transaction.ExecutionContext
import com.lykke.matching.engine.order.transaction.ExecutionContextFactory
import org.slf4j.LoggerFactory
import java.util.*

fun getSetting(value: String, name: String = value) = Setting(name, value, true)

fun getExecutionContext(date: Date, executionContextFactory: ExecutionContextFactory): ExecutionContext {
    return executionContextFactory.create("test",
            "test",
            MessageType.LIMIT_ORDER,
            null,
            emptyMap(),
            date,
            LoggerFactory.getLogger(""),
            LoggerFactory.getLogger(""))
}
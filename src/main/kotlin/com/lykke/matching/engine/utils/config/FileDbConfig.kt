package com.lykke.matching.engine.utils.config

data class FileDbConfig(
        val orderBookPath: String,
        val walletsPath: String,
        val processedMessagesPath: String,
        val processedMessagesInterval: Long
)
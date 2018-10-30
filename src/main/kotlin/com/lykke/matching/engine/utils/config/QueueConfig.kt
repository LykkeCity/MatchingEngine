package com.lykke.matching.engine.utils.config

data class QueueConfig(val queueSizeHealthCheckInterval: Long,
                       val queueSizeLoggerInterval: Long,
                       val queueSizeLimit: Int,
                       val maxQueueSizeLimit: Int,
                       val recoverQueueSizeLimit: Int,
                       val rabbitMaxQueueSizeLimit: Int,
                       val rabbitRecoverQueueSizeLimit: Int)
package com.lykke.matching.engine.outgoing.rabbit.utils

class RabbitEventUtils {
    companion object {
        fun getClientEventConsumerQueueName(exchangeName: String, index: Int): String {
            return "client_queue_${exchangeName}_$index"
        }

        fun getTrustedClientsEventConsumerQueue(exchangeName: String, index: Int): String {
            return "trusted_client_queue_${exchangeName}_$index"
        }
    }
}
package com.lykke.matching.engine.outgoing.rabbit.impl.publishers

import com.rabbitmq.client.AMQP

class RabbitPublishRequest(val routingKey: String,
                           val body: ByteArray,
                           val stringRepresentation: String?,
                           val props: AMQP.BasicProperties)
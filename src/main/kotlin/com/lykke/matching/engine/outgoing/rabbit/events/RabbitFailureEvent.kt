package com.lykke.matching.engine.outgoing.rabbit.events

class RabbitFailureEvent<E>(val publisherName: String, val failedEvent: E?)
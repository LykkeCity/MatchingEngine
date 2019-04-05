package com.lykke.matching.engine.outgoing.messages

class OutgoingEventDataWrapper<T>(val eventClass: Class<T>, val eventData: T)
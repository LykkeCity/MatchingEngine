package com.lykke.matching.engine.outgoing.messages

import com.lykke.matching.engine.daos.bitcoin.Orders

class Swap(val transactionId: String, val clientId1: String, val volume1: String, val asset1: String,
           val clientId2: String, val volume2: String, val asset2: String,
           val orders: Orders): JsonSerializable()
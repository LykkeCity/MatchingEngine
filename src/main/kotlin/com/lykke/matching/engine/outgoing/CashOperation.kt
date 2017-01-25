package com.lykke.matching.engine.outgoing

import java.util.Date

class CashOperation(val id: String, val clientId: String, val dateTime: Date, val volume: String, var asset: String): JsonSerializable()
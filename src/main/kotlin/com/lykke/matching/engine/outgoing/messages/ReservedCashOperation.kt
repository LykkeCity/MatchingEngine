package com.lykke.matching.engine.outgoing.messages

import java.util.Date

// fixme: join with CashOperation ?
class ReservedCashOperation(val id: String, val clientId: String, val dateTime: Date, val reservedVolume: String, var asset: String): JsonSerializable()
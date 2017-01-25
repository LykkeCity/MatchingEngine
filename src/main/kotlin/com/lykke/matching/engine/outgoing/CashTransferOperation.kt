package com.lykke.matching.engine.outgoing

import java.util.Date

class CashTransferOperation(val id: String, val fromClientId: String, val toClientId: String, val dateTime: Date, val volume: String, var asset: String): JsonSerializable()
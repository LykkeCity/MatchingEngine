package com.lykke.matching.engine.daos

import com.lykke.matching.engine.outgoing.JsonSerializable
import java.util.Date

class TransferOperation(val fromClientId: String, val toClientId: String, val uid: String, val assetId: String, val dateTime: Date, val amount: Double): JsonSerializable()
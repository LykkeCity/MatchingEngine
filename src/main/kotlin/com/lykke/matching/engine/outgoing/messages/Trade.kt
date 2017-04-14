package com.lykke.matching.engine.outgoing.messages

import java.util.Date
import java.util.LinkedList

class Trade(val externalID: String, val marketOrderId: String, val timestamp: Date): JsonSerializable() {
    val swaps = LinkedList<Swap>()
}
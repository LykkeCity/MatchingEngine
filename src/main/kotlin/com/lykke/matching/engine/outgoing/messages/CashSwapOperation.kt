package com.lykke.matching.engine.outgoing.messages

import java.util.Date

class CashSwapOperation(val id: String, val dateTime: Date,
                        val clientId1: String, val asset1: String, val volume1: String,
                        val clientId2: String, val asset2: String, val volume2: String,val  messageId: String)
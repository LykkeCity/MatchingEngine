package com.lykke.matching.engine.outgoing.rabbit.events

import com.lykke.matching.engine.outgoing.messages.CashSwapOperation

class CashSwapEvent(val cashSwapOperation: CashSwapOperation)
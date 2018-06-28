package com.lykke.matching.engine.outgoing.rabbit.events

import com.lykke.matching.engine.daos.LkkTrade

class LkkTradesEvent(val trades: List<LkkTrade>)
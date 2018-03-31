package com.lykke.matching.engine.database.cache

import com.lykke.matching.engine.history.TickBlobHolder
import java.util.HashMap

class MarketStateCache {
    private val oneHourMArketStates = HashMap<String, TickBlobHolder>()
    private val oneDayMArketStates = HashMap<String, TickBlobHolder>()
    private val threeDaysMArketStates = HashMap<String, TickBlobHolder>()
    private val oneMonthMArketStates = HashMap<String, TickBlobHolder>()
    private val oneYearMArketStates = HashMap<String, TickBlobHolder>()
}
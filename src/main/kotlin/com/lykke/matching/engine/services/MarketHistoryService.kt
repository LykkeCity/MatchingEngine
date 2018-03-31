package com.lykke.matching.engine.services

import com.lykke.matching.engine.database.HistoryTicksDatabaseAccessor
import com.lykke.matching.engine.history.TickBlobHolder
import com.lykke.utils.logging.PerformanceLogger
import org.apache.log4j.Logger
import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.*
import kotlin.concurrent.fixedRateTimer

class MarketHistoryService(
        private val historyTicksDatabaseAccessor: HistoryTicksDatabaseAccessor,
        private val genericLimitOrderService: GenericLimitOrderService,
        private val frequency: Long) {

    private val COUNT: Long = 4000

    private val ONE_HOUR = "1H"
    private val oneHourTicks = HashMap<String, TickBlobHolder>()

    private val ONE_DAY = "1D"
    private val oneDayTicks = HashMap<String, TickBlobHolder>()
    private var oneDayLastUpdateTime: Long = 0
    private val oneDayUpdateInterval: Long = (24 * 60 * 60 * 1000) / COUNT

    private val THREE_DAYS = "3D"
    private val threeDaysTicks = HashMap<String, TickBlobHolder>()
    private var threeDaysLastUpdateTime: Long = 0
    private val threeDaysUpdateInterval: Long = (3 * 24 * 60 * 60 * 1000) / COUNT

    private val ONE_MONTH = "1M"
    private val oneMonthTicks = HashMap<String, TickBlobHolder>()
    private var oneMonthLastUpdateTime: Long = 0
    private val oneMonthUpdateInterval: Long = (30L * 24 * 60 * 60 * 1000) / COUNT

    private val ONE_YEAR = "1Y"
    private val oneYearTicks = HashMap<String, TickBlobHolder>()
    private var oneYearLastUpdateTime: Long = 0
    private val oneYearUpdateInterval: Long = (365L * 24 * 60 * 60 * 1000) / COUNT

    private val performanceLogger = PerformanceLogger(Logger.getLogger("historyPersistStats"), 10, "buildTicks: ")

    fun init() {
        val blobs = historyTicksDatabaseAccessor.loadHistoryTicks()

        blobs.forEach { blob ->
            blob.downloadAttributes()
            val names = blob.name.split("_")
            val assetPair = names[1]
            val period = names[2]

            when (period) {
                ONE_HOUR -> {
                    oneHourTicks[assetPair] = TickBlobHolder(blob.name, blob)
                }
                ONE_DAY -> {
                    oneDayTicks[assetPair] = TickBlobHolder(blob.name, blob)
                    oneDayLastUpdateTime = blob.properties.lastModified.time
                }
                THREE_DAYS -> {
                    threeDaysTicks[assetPair] = TickBlobHolder(blob.name, blob)
                    threeDaysLastUpdateTime = blob.properties.lastModified.time
                }
                ONE_MONTH -> {
                    oneMonthTicks[assetPair] = TickBlobHolder(blob.name, blob)
                    oneMonthLastUpdateTime = blob.properties.lastModified.time
                }
                ONE_YEAR -> {
                    oneYearTicks[assetPair] = TickBlobHolder(blob.name, blob)
                    oneYearLastUpdateTime = blob.properties.lastModified.time
                }
            }
        }
    }

    fun start(): Timer {

        return fixedRateTimer(name = "HistoryTicksBuilder", initialDelay = 0, period = getInterval(Duration.ofHours(1))) {
            recordCurrentMarketState()
        }
    }

    private fun recordCurrentMarketState() {
        performanceLogger.start()
        val startTimeOfBuildingMarketProfile = Date()
        val bestPrices = genericLimitOrderService.buildMarketProfile()

        bestPrices.forEach { recordMarketStateForAsset(startTimeOfBuildingMarketProfile.time, it.asset, it.ask, it.bid)}

        persist(startTimeOfBuildingMarketProfile)
    }

    private fun persist(startTimeOfBuildingMarketProfile: Date) {
        performanceLogger.startPersist()
        saveTicks(startTimeOfBuildingMarketProfile.time)
        performanceLogger.endPersist()
        performanceLogger.end()
        performanceLogger.fixTime()
    }

    private fun recordMarketStateForAsset(now: Long, asset: String, ask: Double, bid: Double) {
        addTick(oneHourTicks, asset, ONE_HOUR, ask, bid)
        if (oneDayLastUpdateTime + oneDayUpdateInterval < now ) {
            addTick(oneDayTicks, asset, ONE_DAY, ask, bid)
        }
        if (threeDaysLastUpdateTime + threeDaysUpdateInterval < now ) {
            addTick(threeDaysTicks, asset, THREE_DAYS, ask, bid)
        }
        if (oneMonthLastUpdateTime + oneMonthUpdateInterval < now ) {
            addTick(oneMonthTicks, asset, ONE_MONTH, ask, bid)
        }
        if (oneYearLastUpdateTime + oneYearUpdateInterval < now ) {
            addTick(oneYearTicks, asset, ONE_YEAR, ask, bid)
        }        
    }
    
    private fun addTick(ticks: HashMap<String, TickBlobHolder>, asset: String, suffix: String, ask: Double, bid: Double) {
        if (!ticks.containsKey(asset)) {
            val blob = historyTicksDatabaseAccessor.loadHistoryTick(asset, suffix)
            if (blob != null) {
                ticks[asset] =  TickBlobHolder(blob.name, blob)
            } else {
                ticks[asset] = TickBlobHolder("BA_${asset}_$suffix", null)
            }

        }
        ticks[asset]!!.addTick(ask, bid)
    }

    private fun saveTicks(now: Long) {
        if (oneDayLastUpdateTime + oneDayUpdateInterval < now ) {
            oneHourTicks.values.forEach { historyTicksDatabaseAccessor.saveHistoryTick(it) }
            oneDayTicks.values.forEach { historyTicksDatabaseAccessor.saveHistoryTick(it) }
            oneDayLastUpdateTime = now
        }
        if (threeDaysLastUpdateTime + threeDaysUpdateInterval < now ) {
            threeDaysTicks.values.forEach { historyTicksDatabaseAccessor.saveHistoryTick(it) }
            threeDaysLastUpdateTime = now
        }
        if (oneMonthLastUpdateTime + oneMonthUpdateInterval < now ) {
            oneMonthTicks.values.forEach { historyTicksDatabaseAccessor.saveHistoryTick(it) }
            oneMonthLastUpdateTime = now
        }
        if (oneYearLastUpdateTime + oneYearUpdateInterval < now ) {
            oneYearTicks.values.forEach { historyTicksDatabaseAccessor.saveHistoryTick(it) }
            oneYearLastUpdateTime = now
        }
    }

    private fun getInterval(duration: Duration): Long {
        return duration.get(ChronoUnit.MILLIS) / frequency
    }
}

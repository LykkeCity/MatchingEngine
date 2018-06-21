package com.lykke.matching.engine.keepalive

import com.lykke.matching.engine.utils.monitoring.HealthMonitor
import com.lykke.matching.engine.utils.monitoring.MonitoringStatsCollector
import com.lykke.utils.AppVersion
import com.lykke.utils.keepalive.http.IsAliveResponse
import com.lykke.utils.keepalive.http.IsAliveResponseGetter
import org.apache.http.HttpStatus
import org.springframework.context.ApplicationContext

class MeIsAliveResponseGetter(private val healthMonitor: HealthMonitor,
                              private val applicationContext: ApplicationContext): IsAliveResponseGetter() {

    override fun getResponse(): IsAliveResponse {
        val monitoringStatsCollector = applicationContext.getBean(MonitoringStatsCollector::class.java)
        val monitoringResult = monitoringStatsCollector.collectMonitoringResult()

        val ok = healthMonitor.ok()
        val code: Int
        val message: String?
        if (ok) {
            code = HttpStatus.SC_OK
            message = null
        } else {
            code = HttpStatus.SC_INTERNAL_SERVER_ERROR
            message = "Internal Matching Engine error"
        }
        return MeIsAliveResponse(AppVersion.VERSION, code, monitoringResult, message)
    }
}

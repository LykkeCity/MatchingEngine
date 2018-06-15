package com.lykke.matching.engine.keepalive

import com.lykke.matching.engine.utils.monitoring.HealthMonitor
import com.lykke.matching.engine.utils.monitoring.MonitoringStatsCollector
import com.lykke.utils.AppVersion
import com.lykke.utils.keepalive.http.IsAliveResponse
import com.lykke.utils.keepalive.http.IsAliveResponseGetter
import org.apache.http.HttpStatus
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Component


@Component
class MeIsAliveResponseGetter @Autowired constructor (@Qualifier("GeneralHealthMonitor") private val healthMonitor: HealthMonitor,
                                                      private val monitoringStatsCollector: MonitoringStatsCollector): IsAliveResponseGetter() {

    override fun getResponse(): IsAliveResponse {
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

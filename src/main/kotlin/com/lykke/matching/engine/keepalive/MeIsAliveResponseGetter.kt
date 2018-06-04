package com.lykke.matching.engine.keepalive

import com.lykke.matching.engine.utils.monitoring.HealthMonitor
import com.lykke.utils.AppVersion
import com.lykke.utils.keepalive.http.IsAliveResponse
import com.lykke.utils.keepalive.http.IsAliveResponseGetter
import org.apache.http.HttpStatus

class MeIsAliveResponseGetter(private val healthMonitor: HealthMonitor): IsAliveResponseGetter() {

    override fun getResponse(): IsAliveResponse {
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
        return MeIsAliveResponse(AppVersion.VERSION, code, message)
    }

}

private class MeIsAliveResponse(version: String, code: Int, val errorMessage: String?): IsAliveResponse(version, code)
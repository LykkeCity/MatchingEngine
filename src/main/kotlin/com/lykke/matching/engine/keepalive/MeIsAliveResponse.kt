package com.lykke.matching.engine.keepalive

import com.lykke.matching.engine.daos.monitoring.MonitoringResult
import com.lykke.utils.keepalive.http.IsAliveResponse

class MeIsAliveResponse(version: String,
                        code: Int,
                        val monitoringResult: MonitoringResult?,
                        val errorMessage: String?): IsAliveResponse(version, code)
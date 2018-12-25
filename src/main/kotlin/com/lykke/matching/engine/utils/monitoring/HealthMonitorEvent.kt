package com.lykke.matching.engine.utils.monitoring

class HealthMonitorEvent(val ok: Boolean, val component: MonitoredComponent, val qualifier: String? = null)
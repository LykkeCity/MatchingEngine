package com.lykke.matching.engine.outgoing.rabbit.events

import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport

class TrustedLimitOrdersReportEvent(val limitOrdersReport: LimitOrdersReport)
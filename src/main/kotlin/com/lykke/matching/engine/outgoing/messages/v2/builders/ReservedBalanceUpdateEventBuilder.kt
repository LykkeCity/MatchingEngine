package com.lykke.matching.engine.outgoing.messages.v2.builders

import com.lykke.matching.engine.outgoing.messages.v2.enums.MessageType
import com.lykke.matching.engine.outgoing.messages.v2.events.ReservedBalanceUpdateEvent
import com.lykke.matching.engine.outgoing.messages.v2.events.common.BalanceUpdate
import com.lykke.matching.engine.outgoing.messages.v2.events.common.Header
import com.lykke.matching.engine.outgoing.messages.v2.events.common.ReservedBalanceUpdate

class ReservedBalanceUpdateEventBuilder : EventBuilder<ReservedBalanceUpdateData, ReservedBalanceUpdateEvent>() {

    private var balanceUpdates: List<BalanceUpdate>? = null
    private var reservedBalanceUpdate: ReservedBalanceUpdate? = null

    override fun getMessageType() = MessageType.RESERVED_BALANCE_UPDATE

    override fun setEventData(eventData: ReservedBalanceUpdateData): EventBuilder<ReservedBalanceUpdateData, ReservedBalanceUpdateEvent> {
        balanceUpdates = convertBalanceUpdates(eventData.clientBalanceUpdates)
        reservedBalanceUpdate = ReservedBalanceUpdate(eventData.reservedBalanceUpdateOperation.clientId,
                eventData.reservedBalanceUpdateOperation.assetId,
                bigDecimalToString(eventData.reservedBalanceUpdateOperation.reservedAmount)!!)
        return this
    }

    override fun buildEvent(header: Header) = ReservedBalanceUpdateEvent(header, balanceUpdates!!, reservedBalanceUpdate!!)
}
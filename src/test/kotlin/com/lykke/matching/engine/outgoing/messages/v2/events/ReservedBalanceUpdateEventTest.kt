package com.lykke.matching.engine.outgoing.messages.v2.events

import com.lykke.matching.engine.outgoing.messages.v2.enums.MessageType
import com.lykke.matching.engine.outgoing.messages.v2.events.common.BalanceUpdate
import com.lykke.matching.engine.outgoing.messages.v2.events.common.Header
import com.lykke.matching.engine.outgoing.messages.v2.events.common.ReservedBalanceUpdate
import org.junit.Test
import java.util.Date
import kotlin.test.assertEquals


class ReservedBalanceUpdateEventTest {

    @Test
    fun buildGeneratedMessage() {
        val header = Header(MessageType.RESERVED_BALANCE_UPDATE, 1L, "messageUID", "requestUID", "version", Date(), "EVENT_TYPE")
        val balanceUpdates = listOf(BalanceUpdate("Wallet1", "Asset1", "1", "2", "3", "4"),
                BalanceUpdate("Wallet2", "Asset2", "21", "22", "23", "24"))
        val reservedBalanceUpdate = ReservedBalanceUpdate("Wallet3", "Asset3", "-7")
        val event = ReservedBalanceUpdateEvent(header, balanceUpdates, reservedBalanceUpdate)
        val serializedEvent = event.buildGeneratedMessage()

        assertEquals(serializedEvent.header.messageId, "messageUID")
        assertEquals(serializedEvent.header.requestId, "requestUID")
        assertEquals(serializedEvent.header.eventType, "EVENT_TYPE")
        assertEquals(serializedEvent.balanceUpdatesCount, 2)
        assertBalanceUpdate("Wallet1", "Asset1", "1", "2", "3", "4", serializedEvent.balanceUpdatesList)
        assertBalanceUpdate("Wallet2", "Asset2", "21", "22", "23", "24", serializedEvent.balanceUpdatesList)
        assertEquals("Wallet3", serializedEvent.reservedBalanceUpdate.walletId)
        assertEquals("Asset3", serializedEvent.reservedBalanceUpdate.assetId)
        assertEquals("-7", serializedEvent.reservedBalanceUpdate.volume)
    }

}
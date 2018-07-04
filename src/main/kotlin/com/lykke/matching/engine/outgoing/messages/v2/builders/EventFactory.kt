package com.lykke.matching.engine.outgoing.messages.v2.builders

import com.lykke.matching.engine.daos.TransferOperation
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.daos.fee.v2.Fee
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.outgoing.messages.ClientBalanceUpdate
import com.lykke.matching.engine.outgoing.messages.LimitOrderWithTrades
import com.lykke.matching.engine.outgoing.messages.MarketOrderWithTrades
import com.lykke.matching.engine.outgoing.messages.v2.CashInEvent
import com.lykke.matching.engine.outgoing.messages.v2.CashOutEvent
import com.lykke.matching.engine.outgoing.messages.v2.CashTransferEvent
import com.lykke.matching.engine.outgoing.messages.v2.ExecutionEvent
import com.lykke.matching.engine.outgoing.messages.v2.AbstractEvent
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import java.util.Date

class EventFactory {

    companion object {
        private val LOGGER = ThrottlingLogger.getLogger(EventFactory::class.java.name)
        private val METRICS_LOGGER = MetricsLogger.getLogger()

        fun createExecutionEvent(sequenceNumber: Long,
                                 messageId: String,
                                 requestId: String,
                                 date: Date,
                                 messageType: MessageType,
                                 marketOrderWithTrades: MarketOrderWithTrades): ExecutionEvent {
            return createExecutionEvent(sequenceNumber,
                    messageId,
                    requestId,
                    date,
                    messageType,
                    emptyList(),
                    emptyList(),
                    marketOrderWithTrades)
        }

        fun createExecutionEvent(sequenceNumber: Long,
                                 messageId: String,
                                 requestId: String,
                                 date: Date,
                                 messageType: MessageType,
                                 clientBalanceUpdates: List<ClientBalanceUpdate>,
                                 limitOrdersWithTrades: List<LimitOrderWithTrades>,
                                 marketOrderWithTrades: MarketOrderWithTrades? = null): ExecutionEvent {
            return createEvent {
                ExecutionEventBuilder()
                        .setHeaderData(sequenceNumber, messageId, requestId, date, messageType)
                        .setEventData(ExecutionEventData(clientBalanceUpdates, limitOrdersWithTrades, marketOrderWithTrades))
                        .build()
            }
        }

        fun createTrustedClientsExecutionEvent(sequenceNumber: Long,
                                               messageId: String,
                                               requestId: String,
                                               date: Date,
                                               messageType: MessageType,
                                               limitOrdersWithTrades: List<LimitOrderWithTrades>): ExecutionEvent {
            return createEvent {
                ExecutionEventBuilder()
                        .setHeaderData(sequenceNumber, messageId, requestId, date, messageType)
                        .setEventData(ExecutionEventData(emptyList(), limitOrdersWithTrades, null))
                        .build()
            }
        }

        fun createCashInEvent(sequenceNumber: Long,
                              messageId: String,
                              requestId: String,
                              date: Date,
                              messageType: MessageType,
                              clientBalanceUpdates: List<ClientBalanceUpdate>,
                              cashInOperation: WalletOperation,
                              internalFees: List<Fee>): CashInEvent {
            return createEvent {
                CashInEventBuilder()
                        .setHeaderData(sequenceNumber, messageId, requestId, date, messageType)
                        .setEventData(CashInEventData(clientBalanceUpdates, cashInOperation, internalFees))
                        .build()
            }
        }

        fun createCashOutEvent(sequenceNumber: Long,
                               messageId: String,
                               requestId: String,
                               date: Date,
                               messageType: MessageType,
                               clientBalanceUpdates: List<ClientBalanceUpdate>,
                               cashOutOperation: WalletOperation,
                               internalFees: List<Fee>): CashOutEvent {
            return createEvent {
                CashOutEventBuilder()
                        .setHeaderData(sequenceNumber, messageId, requestId, date, messageType)
                        .setEventData(CashOutEventData(clientBalanceUpdates, cashOutOperation, internalFees))
                        .build()
            }
        }

        fun createCashTransferEvent(sequenceNumber: Long,
                                    messageId: String,
                                    requestId: String,
                                    date: Date,
                                    messageType: MessageType,
                                    clientBalanceUpdates: List<ClientBalanceUpdate>,
                                    transferOperation: TransferOperation,
                                    internalFees: List<Fee>): CashTransferEvent {
            return createEvent {
                CashTransferEventBuilder()
                        .setHeaderData(sequenceNumber, messageId, requestId, date, messageType)
                        .setEventData(CashTransferData(clientBalanceUpdates, transferOperation, internalFees))
                        .build()
            }
        }

        private fun <T : AbstractEvent<*>> createEvent(createEvent: () -> T): T {
            return try {
                createEvent()
            } catch (e: Exception) {
                val errorMessage = "Unable to create and send outgoing message: ${e.message}"
                LOGGER.error(errorMessage, e)
                METRICS_LOGGER.logError(errorMessage, e)
                throw e
            }
        }
    }
}
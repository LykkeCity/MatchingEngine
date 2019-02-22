package com.lykke.matching.engine.outgoing.messages.v2.builders

import com.lykke.matching.engine.daos.TransferOperation
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.daos.fee.v2.Fee
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.outgoing.messages.ClientBalanceUpdate
import com.lykke.matching.engine.outgoing.messages.LimitOrderWithTrades
import com.lykke.matching.engine.outgoing.messages.MarketOrderWithTrades
import com.lykke.matching.engine.outgoing.messages.ExecutionEventData
import com.lykke.matching.engine.outgoing.messages.v2.events.CashInEvent
import com.lykke.matching.engine.outgoing.messages.v2.events.CashOutEvent
import com.lykke.matching.engine.outgoing.messages.v2.events.CashTransferEvent
import com.lykke.matching.engine.outgoing.messages.v2.events.ExecutionEvent
import com.lykke.matching.engine.outgoing.messages.v2.events.Event
import com.lykke.matching.engine.outgoing.messages.v2.events.ReservedBalanceUpdateEvent
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import java.math.BigDecimal
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

        fun createCashInOutEvent(volume: BigDecimal,
                                 sequenceNumber: Long,
                                 messageId: String,
                                 requestId: String,
                                 date: Date,
                                 messageType: MessageType,
                                 clientBalanceUpdates: List<ClientBalanceUpdate>,
                                 cashInOperation: WalletOperation,
                                 internalFees: List<Fee>): Event<*> {
            return if (volume > BigDecimal.ZERO) {
                createCashInEvent(sequenceNumber,
                        messageId,
                        requestId,
                        date,
                        messageType,
                        clientBalanceUpdates,
                        cashInOperation,
                        internalFees)
            } else {
                createCashOutEvent(sequenceNumber,
                        messageId,
                        requestId,
                        date,
                        messageType,
                        clientBalanceUpdates,
                        cashInOperation,
                        internalFees)
            }

        }

        private fun createCashInEvent(sequenceNumber: Long,
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

        private fun createCashOutEvent(sequenceNumber: Long,
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

        fun createReservedBalanceUpdateEvent(sequenceNumber: Long,
                                             messageId: String,
                                             requestId: String,
                                             date: Date,
                                             messageType: MessageType,
                                             clientBalanceUpdates: List<ClientBalanceUpdate>,
                                             reservedBalanceUpdateOperation: WalletOperation): ReservedBalanceUpdateEvent {
            return createEvent {
                ReservedBalanceUpdateEventBuilder()
                        .setHeaderData(sequenceNumber, messageId, requestId, date, messageType)
                        .setEventData(ReservedBalanceUpdateData(clientBalanceUpdates, reservedBalanceUpdateOperation))
                        .build()
            }
        }

        private fun <T : Event<*>> createEvent(createEvent: () -> T): T {
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
package com.lykke.matching.engine.utils.config

import com.google.gson.annotations.SerializedName

data class RabbitMqConfigs(
        @SerializedName("OrderBooks")
        val orderBooks: RabbitConfig,
        @SerializedName("CashOperations")
        val cashOperations: RabbitConfig,
        @SerializedName("ReservedCashOperations")
        val reservedCashOperations: RabbitConfig,
        @SerializedName("Transfers")
        val transfers: RabbitConfig,
        @SerializedName("SwapOperations")
        val swapOperations: RabbitConfig,
        @SerializedName("BalanceUpdates")
        val balanceUpdates: RabbitConfig,
        @SerializedName("MarketOrders")
        val marketOrders: RabbitConfig,
        @SerializedName("LimitOrders")
        val limitOrders: RabbitConfig,
        @SerializedName("TrustedLimitOrders")
        val trustedLimitOrders: RabbitConfig,
        val events: Set<RabbitConfig>,
        val trustedClientsEvents: Set<RabbitConfig>,
        @SerializedName("HearBeatTimeout")
        val hearBeatTimeout: Long,
        @SerializedName("HandshakeTimeout")
        val handshakeTimeout: Long
)
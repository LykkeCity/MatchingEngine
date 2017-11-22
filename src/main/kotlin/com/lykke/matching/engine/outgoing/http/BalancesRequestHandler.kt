package com.lykke.matching.engine.outgoing.http

import com.google.gson.GsonBuilder
import com.lykke.matching.engine.daos.wallet.AssetBalance
import com.lykke.matching.engine.holders.BalancesHolder
import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpHandler
import org.apache.log4j.Logger
import java.util.LinkedList

private data class Wallet(
        val clientId: String,
        val balances: List<AssetBalance>
)

class BalancesRequestHandler(private val balancesHolder: BalancesHolder) : HttpHandler {

    companion object {
        private val LOGGER = Logger.getLogger(BalancesRequestHandler::class.java.name)
    }

    override fun handle(exchange: HttpExchange) {
        try {
            val balances = LinkedList<Wallet>()
            balancesHolder.wallets.values.forEach {
                balances.add(Wallet(it.clientId, it.balances.values.toList()))
            }
            val response = GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ").create().toJson(balances)
            exchange.sendResponseHeaders(200, response.length.toLong())
            val os = exchange.responseBody
            os.write(response.toByteArray())
            os.close()
            LOGGER.info("Balances snapshot sent to ${exchange.remoteAddress}")
        } catch (e: Exception) {
            LOGGER.error("Unable to write balances snapshot response to ${exchange.remoteAddress}", e)
        }
    }

}
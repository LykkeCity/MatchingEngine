package com.lykke.matching.engine.web.controllers

import com.lykke.matching.engine.holders.BalancesHolder
import io.swagger.annotations.Api
import io.swagger.annotations.ApiOperation
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import java.math.BigDecimal

@RestController
@Api(description = "Read only endpoint that enables to get balance/reserved balance of the client")
class BalancesController {

    @Autowired
    private lateinit var balancesHolder: BalancesHolder

    @GetMapping("/balance")
    @ApiOperation("Returns balance of supplied asset for client")
    fun getBalance(@RequestParam("clientId") clientId: String,
                   @RequestParam("assetId") assetId: String): BigDecimal {
        return balancesHolder.getBalance(clientId, assetId)
    }

    @GetMapping("/balances")
    @ApiOperation("Returns balances for all assets of supplied client")
    fun getBalances(@RequestParam("clientId") clientId: String): Map<String, BigDecimal> {
        return balancesHolder.getBalances(clientId).mapValues { entry -> entry.value.balance}
    }

    @ApiOperation("Returns reserved balance of supplied asset for client")
    @GetMapping("/balance/reserved")
    fun getReservedBalance(@RequestParam("clientId") client: String, @RequestParam("assetId") assetId: String): BigDecimal {
        return balancesHolder.getReservedBalance(client, assetId)
    }

    @ApiOperation("Returns reserved balances for all assets of supplied client")
    @GetMapping("/balances/reserved")
    fun getReservedBalances(@RequestParam("clientId") client: String): Map<String, BigDecimal> {
        return balancesHolder.getBalances(client).mapValues { entry -> entry.value.reserved }
    }
}
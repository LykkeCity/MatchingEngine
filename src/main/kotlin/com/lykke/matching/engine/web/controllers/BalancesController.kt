package com.lykke.matching.engine.web.controllers

import com.lykke.matching.engine.daos.wallet.AssetBalance
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.web.dto.BalanceDto
import io.swagger.annotations.Api
import io.swagger.annotations.ApiOperation
import org.apache.commons.lang3.StringUtils
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController

@RestController
@Api(description = "Read only endpoint that enables to get balance/reserved balance of the client")
class BalancesController {
    @Autowired
    private lateinit var balancesHolder: BalancesHolder

    @GetMapping("/balances")
    @ApiOperation("Returns balances/reserved balances of supplied client and assetId")
    fun getBalances(@RequestParam("clientId") clientId: String,
                    @RequestParam(name = "assetId", required = false, defaultValue = "") assetId: String): Map<String, BalanceDto?> {

        if (StringUtils.isNoneBlank(assetId)) {
            return mapOf(assetId to toBalanceDto(balancesHolder.getBalances(clientId)[assetId]))
        }

        return balancesHolder.getBalances(clientId).mapValues { entry -> toBalanceDto(entry.value) }
    }

    private fun toBalanceDto(assetBalance: AssetBalance?): BalanceDto? {
        return BalanceDto(assetBalance?.balance, assetBalance?.reserved)
    }
}
package com.lykke.matching.engine.web.controllers

import com.lykke.matching.engine.daos.wallet.AssetBalance
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.web.dto.BalanceDto
import io.swagger.annotations.Api
import io.swagger.annotations.ApiOperation
import org.apache.commons.lang3.StringUtils
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController

@RestController
@Api(description = "Read only api, returns balance information for supplied client")
class BalancesController {
    @Autowired
    private lateinit var balancesHolder: BalancesHolder

    @GetMapping("/balances", produces = [MediaType.APPLICATION_JSON_VALUE])
    @ApiOperation("Returns balance information for supplied client and assetId")
    fun getBalances(@RequestParam("clientId") clientId: String,
                    @RequestParam(name = "assetId", required = false, defaultValue = "") assetId: String): List<BalanceDto?>  {

        if (StringUtils.isNoneBlank(assetId)) {
            return listOf(toBalanceDto(assetId, balancesHolder.getBalances(clientId)[assetId]))
        }

        return balancesHolder
                .getBalances(clientId)
                .mapValues { entry -> toBalanceDto(entry.value.asset, entry.value) }
                .values
                .toList()
    }

    private fun toBalanceDto(assetId: String, assetBalance: AssetBalance?): BalanceDto? {
        return BalanceDto(assetId, assetBalance?.balance, assetBalance?.reserved)
    }
}
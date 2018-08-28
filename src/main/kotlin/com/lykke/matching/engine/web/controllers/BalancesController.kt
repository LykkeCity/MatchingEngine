package com.lykke.matching.engine.web.controllers

import com.lykke.matching.engine.daos.wallet.AssetBalance
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.web.dto.Balance
import io.swagger.annotations.Api
import io.swagger.annotations.ApiOperation
import org.apache.commons.lang3.StringUtils
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.*

@RestController
@Api(description = "Read only endpoint, returns balance information for supplied client")
class BalancesController {
    @Autowired
    private lateinit var balancesHolder: BalancesHolder

    @GetMapping("/balances", produces = [MediaType.APPLICATION_JSON_VALUE])
    @ApiOperation("Returns balance information for supplied client and assetId")
    fun getBalances(@RequestParam("clientId") clientId: String,
                    @RequestParam(name = "assetId", required = false, defaultValue = "") assetId: String): List<Balance?>  {

        if (StringUtils.isNoneBlank(assetId)) {
            return listOf(toBalanceDto(assetId, balancesHolder.getBalances(clientId)[assetId]))
        }

        return balancesHolder
                .getBalances(clientId)
                .mapValues { entry -> toBalanceDto(entry.value.asset, entry.value) }
                .values
                .toList()
    }

    private fun toBalanceDto(assetId: String, assetBalance: AssetBalance?): Balance? {
        return Balance(assetId, assetBalance?.balance, assetBalance?.reserved)
    }
}
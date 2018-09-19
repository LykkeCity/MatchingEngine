package com.lykke.matching.engine.web.controllers

import com.lykke.matching.engine.daos.wallet.AssetBalance
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.web.dto.BalancesDto
import io.swagger.annotations.Api
import io.swagger.annotations.ApiOperation
import org.apache.commons.lang3.StringUtils
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
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
                    @RequestParam(name = "assetId", required = false, defaultValue = "") assetId: String): ResponseEntity<*> {

        val balances = balancesHolder.getBalances(clientId)

        if (balances.isEmpty()) {
            return ResponseEntity("Requested client has no balances", HttpStatus.NOT_FOUND)
        }

        if (StringUtils.isNoneBlank(assetId)) {
            val clientBalance = balances[assetId] ?: return ResponseEntity("No balance found for client, for supplied asset", HttpStatus.NOT_FOUND)
            return ResponseEntity.ok(listOf(toBalanceDto(assetId, clientBalance)))
        }

        return ResponseEntity.ok(balances
                .mapValues { entry -> toBalanceDto(entry.value.asset, entry.value) }
                .values
                .toList())
    }

    private fun toBalanceDto(assetId: String, assetBalance: AssetBalance): BalancesDto? {
        return BalancesDto(assetId, assetBalance.balance, assetBalance.reserved)
    }
}
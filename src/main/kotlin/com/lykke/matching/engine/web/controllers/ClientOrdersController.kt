package com.lykke.matching.engine.web.controllers

import com.lykke.matching.engine.database.cache.AssetPairsCache
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.services.GenericStopLimitOrderService
import com.lykke.matching.engine.web.dto.ClientOrdersDto
import io.swagger.annotations.Api
import io.swagger.annotations.ApiOperation
import io.swagger.annotations.ApiResponse
import io.swagger.annotations.ApiResponses
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import javax.servlet.http.HttpServletRequest

@RestController
@Api(description = "Read only api to access orders from order book")
class ClientOrdersController {

    @Autowired
    private lateinit var genericLimitOrderService: GenericLimitOrderService

    @Autowired
    private lateinit var genericStopLimitOrderService: GenericStopLimitOrderService

    @Autowired
    private lateinit var assetPairsCache: AssetPairsCache

    @Autowired
    private lateinit var balancesHolder: BalancesHolder

    @GetMapping("wallet/{walletId}/orders", produces = [MediaType.APPLICATION_JSON_VALUE])
    @ApiOperation("Endpoint to access orders from current order book for given wallet of the client")
    @ApiResponses(
            ApiResponse(code = 200, message = "Success"),
            ApiResponse(code = 400, message = "Supplied asset id is not supported"),
            ApiResponse(code = 404, message = "Wallet not found"),
            ApiResponse(code = 500, message = "Internal server error occurred")
    )
    fun getOrders(@PathVariable("walletId") walletId: String,
                  @RequestParam(required = false) assetPairId: String?,
                  @RequestParam(required = false) isBuy: Boolean?): ResponseEntity<ClientOrdersDto> {

        if (!balancesHolder.clientExists(walletId)) {
            throw WalletNotFoundException()
        }

        if (assetPairId != null && assetPairsCache.getAssetPair(assetPairId) == null) {
            throw IllegalArgumentException("Asset pair is not supported")
        }

        val limitOrders = genericLimitOrderService.searchOrders(walletId, assetPairId, isBuy)
        val stopOrders = genericStopLimitOrderService.searchOrders(walletId, assetPairId, isBuy)

        return ResponseEntity.ok(ClientOrdersDto(limitOrders, stopOrders))
    }

    @ExceptionHandler
    private fun handleIllegalArgumentException(request: HttpServletRequest, exception: IllegalArgumentException): ResponseEntity<String> {
        return ResponseEntity(exception.message ?: "", HttpStatus.BAD_REQUEST)
    }

    @ExceptionHandler
    private fun handleWalletNotFoundException(request: HttpServletRequest, exception: WalletNotFoundException): ResponseEntity<String> {
        return ResponseEntity("Wallet is not found - there is no balances data for given wallet", HttpStatus.NOT_FOUND)
    }

    class WalletNotFoundException(): Exception()
}
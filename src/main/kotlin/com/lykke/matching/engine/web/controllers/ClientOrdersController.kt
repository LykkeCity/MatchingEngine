package com.lykke.matching.engine.web.controllers

import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.services.GenericStopLimitOrderService
import com.lykke.matching.engine.web.dto.ClientOrdersDto
import io.swagger.annotations.Api
import io.swagger.annotations.ApiOperation
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController

@RestController
@Api(description = "Read only api to access orders from order book")
class ClientOrdersController {

    @Autowired
    private lateinit var genericLimitOrderService: GenericLimitOrderService

    @Autowired
    private lateinit var genericStopLimitOrderService: GenericStopLimitOrderService

    @GetMapping("client/{clientId}/orders", produces = [MediaType.APPLICATION_JSON_VALUE])
    @ApiOperation("Endpoint to access orders from current order book for given client")
    fun getOrders(@PathVariable("clientId") clientId: String,
                  @RequestParam(required = false) assetPair: String?,
                  @RequestParam(required = false) isBuy: Boolean?): ClientOrdersDto {
        val limitOrders = genericLimitOrderService.searchOrders(clientId, assetPair, isBuy)
        val stopOrders = genericStopLimitOrderService.searchOrders(clientId, assetPair, isBuy)

        return ClientOrdersDto(limitOrders, stopOrders)
    }
}
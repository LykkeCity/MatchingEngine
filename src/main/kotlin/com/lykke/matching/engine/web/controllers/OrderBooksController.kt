package com.lykke.matching.engine.web.controllers

import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RestController

@RestController
class OrderBooksController {
    @GetMapping("/orderBooks")
    fun getOrderBooks() {

    }

    @GetMapping("/stopOrderBooks")
    fun getStopOrderBooks() {

    }
}
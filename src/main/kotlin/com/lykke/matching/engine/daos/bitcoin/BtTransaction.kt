package com.lykke.matching.engine.daos.bitcoin

import java.util.Date

class BtTransaction(val id: String, val created: Date, val requestData: String, val clientCashOperationPair: ClientCashOperationPair? = null, val orders: Orders? = null)
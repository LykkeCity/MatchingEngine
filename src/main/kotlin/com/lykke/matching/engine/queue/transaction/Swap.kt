package com.lykke.matching.engine.queue.transaction

import com.lykke.matching.engine.daos.Orders

class Swap(var TransactionId: String? = null, var MultisigCustomer1: String? = null, val Amount1: Double, var Asset1: String? = null,
           var MultisigCustomer2: String? = null, val Amount2: Double, var Asset2: String? = null,
           @Transient var clientId1: String, @Transient var origAsset1: String,
           @Transient var clientId2: String, @Transient var origAsset2: String,
           @Transient var orders: Orders): Transaction {
    //Swap:{"TransactionId":"10", MultisigCustomer1:"2N8zbehwdz2wcCd2JwZktnt6uKZ8fFMZVPp", "Amount1":200, "Asset1":"TestExchangeUSD", MultisigCustomer2:"2N8Z7FLao3qWc8h8mveDXaAA9q1Q53xMsyL", "Amount2":300, "Asset2":"TestExchangeEUR"}
}
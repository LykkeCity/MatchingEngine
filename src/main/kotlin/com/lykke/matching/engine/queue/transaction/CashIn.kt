package com.lykke.matching.engine.queue.transaction

class CashIn(var TransactionId: String? = null, var MultisigAddress: String? = null, var Amount: Double, var Currency: String,
             @Transient var clientId: String, @Transient var cashOperationId: String): Transaction {

    //CashIn:{"TransactionId":"10","MultisigAddress":"3NQ6FF3n8jPFyPewMqzi2qYp8Y4p3UEz9B","Amount":5000,"Currency":"bjkUSD"}
}
package com.lykke.matching.engine.queue.transaction

class CashOut(var TransactionId: String? = null, var MultisigAddress: String? = null, val Amount: Double, var Currency: String, var PrivateKey: String? = null,
              @Transient var clientId: String, @Transient var cashOperationId: String): Transaction {
    //CashOut:{"TransactionId":"10","MultisigAddress":"2NC9qfGybmWgKUdfSebana1HPsAUcXvMmpo","Amount":200,"Currency":"bjkUSD","PrivateKey":"xxx"}
}
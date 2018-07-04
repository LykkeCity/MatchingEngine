package com.lykke.matching.engine.outgoing.messages.v2.builders

import com.lykke.matching.engine.daos.TransferOperation
import com.lykke.matching.engine.daos.fee.v2.Fee
import com.lykke.matching.engine.outgoing.messages.ClientBalanceUpdate

class CashTransferData(val clientBalanceUpdates: List<ClientBalanceUpdate>,
                       val transferOperation: TransferOperation,
                       val internalFees: List<Fee>): EventData
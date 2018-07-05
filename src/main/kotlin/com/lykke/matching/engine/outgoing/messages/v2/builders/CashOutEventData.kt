package com.lykke.matching.engine.outgoing.messages.v2.builders

import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.daos.fee.v2.Fee
import com.lykke.matching.engine.outgoing.messages.ClientBalanceUpdate

class CashOutEventData(val clientBalanceUpdates: List<ClientBalanceUpdate>,
                       val cashOutOperation: WalletOperation,
                       val internalFees: List<Fee>): EventData
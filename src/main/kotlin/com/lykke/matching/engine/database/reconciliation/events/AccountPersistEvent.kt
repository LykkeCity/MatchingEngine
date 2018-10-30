package com.lykke.matching.engine.database.reconciliation.events

import com.lykke.matching.engine.daos.wallet.Wallet

class AccountPersistEvent(val persistenceData: Collection<Wallet>)
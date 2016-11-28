package com.lykke.matching.engine.daos.bitcoin

class Orders(var marketOrder: ClientOrderPair, var limitOrder: ClientOrderPair, var trades: Array<ClientTradePair>)

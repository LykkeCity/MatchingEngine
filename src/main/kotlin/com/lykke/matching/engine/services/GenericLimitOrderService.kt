package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.BestPrice
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.holders.OrdersDatabaseAccessorsHolder
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.order.OrderStatus.Cancelled
import com.lykke.matching.engine.order.transaction.CurrentTransactionOrderBooksHolder
import com.lykke.matching.engine.utils.NumberUtils
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.util.ArrayList
import java.util.Date
import java.util.HashMap
import java.util.LinkedList
import java.util.concurrent.BlockingQueue
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.PriorityBlockingQueue

@Component
class GenericLimitOrderService @Autowired constructor(private val orderBookDatabaseAccessorHolder: OrdersDatabaseAccessorsHolder,
                                                      private val assetsHolder: AssetsHolder,
                                                      private val assetsPairsHolder: AssetsPairsHolder,
                                                      private val balancesHolder: BalancesHolder,
                                                      private val tradeInfoQueue: BlockingQueue<TradeInfo>) : AbstractGenericLimitOrderService<AssetOrderBook> {

    companion object {
        private val LOGGER = Logger.getLogger(GenericLimitOrderService::class.java.name)
    }

    //asset -> orderBook
    private val limitOrdersQueues = ConcurrentHashMap<String, AssetOrderBook>()
    private val limitOrdersMap = HashMap<String, LimitOrder>()
    private val clientLimitOrdersMap = HashMap<String, MutableList<LimitOrder>>()
    var initialOrdersCount = 0

    init {
        update()
    }

    fun update() {
        limitOrdersQueues.clear()
        limitOrdersMap.clear()
        clientLimitOrdersMap.clear()
        val orders = orderBookDatabaseAccessorHolder.primaryAccessor.loadLimitOrders()
        for (order in orders) {
            addToOrderBook(order)
        }
        initialOrdersCount = orders.size
    }

    private fun addToOrderBook(order: LimitOrder) {
        val orderBook = limitOrdersQueues.getOrPut(order.assetPairId) { AssetOrderBook(order.assetPairId) }
        orderBook.addOrder(order)
        addOrder(order)
    }

    fun addOrder(order: LimitOrder) {
        limitOrdersMap[order.externalId] = order
        clientLimitOrdersMap.getOrPut(order.clientId) { ArrayList() }.add(order)
    }

    override fun addOrders(orders: Collection<LimitOrder>) {
        orders.forEach { order ->
            addOrder(order)
        }
    }

    fun getAllOrderBooks() = limitOrdersQueues

    override fun getOrderBook(assetPairId: String) = limitOrdersQueues[assetPairId] ?: AssetOrderBook(assetPairId)

    override fun setOrderBook(assetPairId: String, assetOrderBook: AssetOrderBook) {
        limitOrdersQueues[assetPairId] = assetOrderBook
    }

    fun setOrderBook(assetPair: String, isBuy: Boolean, book: PriorityBlockingQueue<LimitOrder>) {
        limitOrdersQueues.getOrPut(assetPair) { AssetOrderBook(assetPair) }.setOrderBook(isBuy, book)
    }

    fun checkAndReduceBalance(order: LimitOrder, volume: BigDecimal, limitBalances: MutableMap<String, BigDecimal>): Boolean {
        val assetPair = assetsPairsHolder.getAssetPair(order.assetPairId)
        val limitAssetId = if (order.isBuySide()) assetPair.quotingAssetId else assetPair.baseAssetId
        val availableBalance = limitBalances[order.clientId] ?: balancesHolder.getAvailableReservedBalance(order.clientId, limitAssetId)
        val accuracy = assetsHolder.getAsset(limitAssetId).accuracy
        val result = availableBalance >= volume
        LOGGER.debug("order=${order.externalId}, client=${order.clientId}, $limitAssetId : ${NumberUtils.roundForPrint(availableBalance)} >= ${NumberUtils.roundForPrint(volume)} = $result")
        if (result) {
            limitBalances[order.clientId] = NumberUtils.setScaleRoundHalfUp(availableBalance - volume, accuracy)
        }
        return result
    }

    fun getOrder(uid: String) = limitOrdersMap[uid]

    fun searchOrders(clientId: String, assetPair: String?, isBuy: Boolean?): List<LimitOrder> {
        val result = mutableListOf<LimitOrder>()
        clientLimitOrdersMap[clientId]?.forEach { limitOrder ->
            if ((assetPair == null || limitOrder.assetPairId == assetPair) && (isBuy == null || limitOrder.isBuySide() == isBuy)) {
                result.add(limitOrder)
            }
        }
        return result
    }

    fun cancelLimitOrder(date: Date, uid: String, removeFromClientMap: Boolean = false): LimitOrder? {
        val order = limitOrdersMap.remove(uid) ?: return null

        if (removeFromClientMap) {
            removeFromClientMap(uid)
        }

        getOrderBook(order.assetPairId).removeOrder(order)
        order.updateStatus(Cancelled, date)
        return order
    }

    private fun removeFromClientMap(uid: String): Boolean {
        val order: LimitOrder = clientLimitOrdersMap.values.firstOrNull { it.any { it.externalId == uid } }?.firstOrNull { it.externalId == uid } ?: return false
        return clientLimitOrdersMap[order.clientId]?.remove(order) ?: false
    }

    override fun cancelLimitOrders(orders: Collection<LimitOrder>, date: Date) {
        orders.forEach { order ->
            val ord = limitOrdersMap.remove(order.externalId)
            clientLimitOrdersMap[order.clientId]?.remove(order)
            if (ord != null) {
                ord.updateStatus(Cancelled, date)
            }
        }
    }

    override fun removeOrdersFromMapsAndSetStatus(orders: Collection<LimitOrder>, status: OrderStatus?, date: Date?) {
        orders.forEach { order ->
            val removedOrder = limitOrdersMap.remove(order.externalId)
            clientLimitOrdersMap[order.clientId]?.remove(removedOrder)
            if (removedOrder != null && status != null) {
                removedOrder.updateStatus(status, date!!)
            }
        }
    }

    fun putTradeInfo(tradeInfo: TradeInfo) {
        tradeInfoQueue.put(tradeInfo)
    }

    fun buildMarketProfile(): List<BestPrice> {
        val result = LinkedList<BestPrice>()

        limitOrdersQueues.values.forEach { book ->
            val askPrice = book.getAskPrice()
            val bidPrice = book.getBidPrice()
            if (askPrice > BigDecimal.ZERO || bidPrice > BigDecimal.ZERO) {
                result.add(BestPrice(book.assetPairId, askPrice, bidPrice))
            }
        }

        return result
    }

    fun createCurrentTransactionOrderBooksHolder() = CurrentTransactionOrderBooksHolder(this)
}

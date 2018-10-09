package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.BestPrice
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.holders.OrdersDatabaseAccessorsHolder
import com.lykke.matching.engine.order.ExpiryOrdersQueue
import com.lykke.matching.engine.order.OrderStatus.Cancelled
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.util.*
import java.util.concurrent.BlockingQueue
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.PriorityBlockingQueue

@Component
class GenericLimitOrderService @Autowired constructor(private val orderBookDatabaseAccessorHolder: OrdersDatabaseAccessorsHolder,
                                                      assetsPairsHolder: AssetsPairsHolder,
                                                      balancesHolder: BalancesHolder,
                                                      private val tradeInfoQueue: BlockingQueue<TradeInfo>,
                                                      applicationSettingsCache: ApplicationSettingsCache,
                                                      private val expiryOrdersQueue: ExpiryOrdersQueue) : AbstractGenericLimitOrderService<AssetOrderBook> {

    companion object {
        private val LOGGER = Logger.getLogger(GenericLimitOrderService::class.java.name)
    }

    //asset -> orderBook
    private val limitOrdersQueues = ConcurrentHashMap<String, AssetOrderBook>()
    private val limitOrdersMap = HashMap<String, LimitOrder>()
    private val clientLimitOrdersMap = HashMap<String, MutableList<LimitOrder>>()
    private val walletOperationsCalculator: WalletOperationsCalculator = WalletOperationsCalculator(assetsPairsHolder, balancesHolder, applicationSettingsCache)
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
        expiryOrdersQueue.addOrder(order)
    }

    fun addOrders(orders: List<LimitOrder>) {
        orders.forEach { order ->
            addOrder(order)
        }
    }

    fun moveOrdersToDone(orders: List<LimitOrder>) {
        orders.forEach { order ->
            limitOrdersMap.remove(order.externalId)
            clientLimitOrdersMap[order.clientId]?.remove(order)
            expiryOrdersQueue.removeOrder(order)
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

    fun getOrder(uid: String) = limitOrdersMap[uid]

    fun searchOrders(clientId: String, assetPair: String?, isBuy: Boolean?): List<LimitOrder> {
        val result = LinkedList<LimitOrder>()
        clientLimitOrdersMap[clientId]?.forEach { limitOrder ->
            if (limitOrder.assetPairId == (assetPair ?: limitOrder.assetPairId) && limitOrder.isBuySide() == (isBuy ?: limitOrder.isBuySide())) {
                result.add(limitOrder)
            }
        }
        return result
    }

    fun cancelLimitOrder(date: Date, uid: String, removeFromClientMap: Boolean = false): LimitOrder? {
        val order = limitOrdersMap.remove(uid) ?: return null
        expiryOrdersQueue.removeOrder(order)

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
            expiryOrdersQueue.removeOrder(order)
            clientLimitOrdersMap[order.clientId]?.remove(order)
            if (ord != null) {
                ord.updateStatus(Cancelled, date)
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

    fun calculateWalletOperationsForCancelledOrders(orders: List<LimitOrder>): CancelledOrdersOperationsResult {
        return walletOperationsCalculator.calculateForCancelledOrders(orders)
    }
}

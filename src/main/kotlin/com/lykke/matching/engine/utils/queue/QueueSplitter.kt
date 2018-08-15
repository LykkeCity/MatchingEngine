package com.lykke.matching.engine.utils.queue

import java.util.concurrent.BlockingQueue

class QueueSplitter<E>(
        name: String,
        val sourceQueue: BlockingQueue<E>,
        val destinationQueues: Set<BlockingQueue<E>>
): Thread(name) {
    override fun run() {
        while (true) {
            val item = sourceQueue.take()
            for (destinationQueue in destinationQueues) {
                destinationQueue.put(item)
            }
        }
    }
}
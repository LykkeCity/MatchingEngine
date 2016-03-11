package com.lykke.matching.engine.queue

import java.util.LinkedList

class TestQueueWriter: QueueWriter {
    private val queue = LinkedList<String>()

    override fun write(data: String) {
        queue.push(data)
    }

    fun read(): String {
        return queue.pop()
    }
}
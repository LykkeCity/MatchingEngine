package com.lykke.matching.engine.holders

import java.util.LinkedList
import java.util.UUID

class TestUUIDHolder: UUIDHolder {

    val uuids = LinkedList<String>()

    override fun getNextValue(): String {
        return uuids.poll() ?: UUID.randomUUID().toString()
    }

}
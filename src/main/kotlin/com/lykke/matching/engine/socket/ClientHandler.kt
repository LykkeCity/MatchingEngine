package com.lykke.matching.engine.socket

interface ClientHandler {
    fun writeOutput(byteArray: ByteArray)
    fun isConnected(): Boolean
    fun disconnect()
    var clientHostName: String?
}
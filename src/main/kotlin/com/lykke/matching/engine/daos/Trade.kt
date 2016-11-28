package com.lykke.matching.engine.daos

import java.text.SimpleDateFormat
import java.util.Date
import java.util.TimeZone

data class Trade(val clientId: String, val uid: String, val multisig: String, val assetId: String, val dateTime: Date, val limitOrderId: String, val marketOrderId: String, val volume: Double, val price: Double, val addressFrom: String, val addressTo: String) {

    companion object {
        private val DATE_FORMAT = initTimeFormatter()
        private var counter: Long = 0

        fun generateId(date: Date): String {
            counter = ++counter % 99999
            return String.format("%s_%05d", DATE_FORMAT.format(date), counter)
        }

        private fun initTimeFormatter(): SimpleDateFormat {
            val format = SimpleDateFormat("yyyyMMddHHmmssSSS")
            format.timeZone = TimeZone.getTimeZone("UTC")
            return format
        }
    }
}
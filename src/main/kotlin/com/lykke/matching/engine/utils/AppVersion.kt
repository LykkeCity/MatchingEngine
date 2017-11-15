package com.lykke.matching.engine.utils

import java.net.InetAddress
import java.text.SimpleDateFormat
import java.util.Date
import java.util.jar.Attributes
import java.util.jar.Manifest

object AppVersion {

    val REVISION_NUMBER: String?
    val BUILD_NUMBER: String?
    val VERSION: String?

    val INSTANCE_UNIQUE_ID: String?

    init {

        val attributes = Attributes()
        try {
            val urls = AppVersion::class.java.classLoader.getResources("META-INF/MANIFEST.MF")
            while (urls.hasMoreElements()) {
                try {
                    val manifest = Manifest(urls.nextElement().openStream())
                    attributes.putAll(manifest.mainAttributes)
                } catch (e: Exception) {
                    println("Fail to read manifest file: " + e.message)
                }
            }
        } catch (e: Exception) {
            println("Error while reading manifest: " + e.message)
            e.printStackTrace()
        }

        REVISION_NUMBER = attributes.getValue("Revision-number")
        BUILD_NUMBER = attributes.getValue("Build-number")
        VERSION = attributes.getValue("Version") ?: "local"
    }

    init {
        var prefix: String
        try {
            prefix = InetAddress.getLocalHost().hostName + " "
        } catch (t: Throwable) {
            prefix = ""
        }

        val sdf = SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
        INSTANCE_UNIQUE_ID = prefix + sdf.format(Date()) + "." + System.nanoTime() % 1000000L
    }
}

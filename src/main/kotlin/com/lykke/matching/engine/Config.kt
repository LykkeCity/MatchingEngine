package com.lykke.matching.engine

import java.io.FileInputStream
import java.util.Properties

fun loadConfig(path: String): Properties {
    var props = Properties()
    props.load(FileInputStream(path))
    return props
}

fun Properties.getInt(key: String): Int? {
    return this.getProperty(key)?.toInt()
}

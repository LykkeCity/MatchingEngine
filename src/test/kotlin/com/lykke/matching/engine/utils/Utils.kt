package com.lykke.matching.engine.utils

import com.lykke.matching.engine.daos.setting.Setting

fun getSetting(value: String, name: String = value) = Setting(name, value, true, "test")
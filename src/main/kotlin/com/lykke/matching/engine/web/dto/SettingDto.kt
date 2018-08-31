package com.lykke.matching.engine.web.dto

import javax.validation.constraints.NotNull

data class SettingDto (@get:NotNull(message = "Name should not be null") val name: String,
                       @get:NotNull(message = "Value should not be null") val value: String,
                       @get:NotNull(message = "Enabled flag should not be null") val enabled: Boolean?,
                       @get:NotNull(message = "Comment should not be null") val comment: String)
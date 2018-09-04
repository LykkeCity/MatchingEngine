package com.lykke.matching.engine.web.dto

import io.swagger.annotations.ApiModelProperty
import java.util.*
import javax.validation.constraints.NotNull

data class SettingDto(@get:NotNull(message = "Name should not be null") val name: String,
                      @get:NotNull(message = "Value should not be null") val value: String,
                      @get:NotNull(message = "Enabled flag should not be null") val enabled: Boolean?,
                      @get:NotNull(message = "Comment should not be null") val comment: String? = null,
                      @get:NotNull(message = "User should not be null") val user: String? = null,
                      @ApiModelProperty(readOnly = true)
                      val timestamp: Date? = null) {
    constructor(sourceSettingDto: SettingDto, comment: String) : this(sourceSettingDto.name, sourceSettingDto.value, sourceSettingDto.enabled, comment, sourceSettingDto.user, sourceSettingDto.timestamp)
}
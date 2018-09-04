package com.lykke.matching.engine.web.dto

import io.swagger.annotations.ApiModelProperty
import java.util.*
import javax.validation.constraints.NotEmpty
import javax.validation.constraints.NotNull

data class SettingDto(
        @get:NotEmpty(message = "Name should not be empty")
        val name: String,

        @get:NotEmpty(message = "Value should not be empty")
        val value: String,

        @get:NotNull(message = "Enabled flag should not be null")
        val enabled: Boolean?,

        @get:NotEmpty(message = "Comment should not be empty")
        val comment: String? = null,

        @get:NotEmpty(message = "User should not be empty")
        val user: String? = null,

        @ApiModelProperty(readOnly = true)
        val timestamp: Date? = null) {
    constructor(sourceSettingDto: SettingDto, comment: String) : this(sourceSettingDto.name, sourceSettingDto.value, sourceSettingDto.enabled, comment, sourceSettingDto.user, sourceSettingDto.timestamp)
}
package com.lykke.matching.engine.web.dto

import io.swagger.annotations.ApiModelProperty
import java.util.*
import javax.validation.constraints.NotEmpty
import javax.validation.constraints.NotNull

class DisabledFunctionalityRuleDto(
        @ApiModelProperty(readOnly = true)
        val id: String? = null,
        val assetId: String?,
        val assetPairId: String?,
        val messageTypeId: Int?,

        @get:NotNull(message = "Enabled flag should not be null")
        val enabled: Boolean?,

        @get:NotEmpty(message = "Comment should not be empty")
        val comment: String? = null,

        @get:NotEmpty(message = "User should not be empty")
        val user: String? = null,

        @ApiModelProperty(readOnly = true)
        val timestamp: Date? = null)

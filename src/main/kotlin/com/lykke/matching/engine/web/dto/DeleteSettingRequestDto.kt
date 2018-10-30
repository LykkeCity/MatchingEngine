package com.lykke.matching.engine.web.dto

import javax.validation.constraints.NotEmpty

data class DeleteSettingRequestDto(
        @get:NotEmpty(message = "Comment can not be empty")
        val comment: String,
        @get:NotEmpty(message = "User can not be empty")
        val user: String)
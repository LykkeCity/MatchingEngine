package com.lykke.matching.engine.web.controllers

import com.lykke.matching.engine.daos.setting.SettingNotFoundException
import com.lykke.matching.engine.services.DisabledFunctionalityRulesService
import com.lykke.matching.engine.services.validators.impl.ValidationException
import com.lykke.matching.engine.web.dto.DeleteSettingRequestDto
import com.lykke.matching.engine.web.dto.DisabledFunctionalityRuleDto
import io.swagger.annotations.Api
import io.swagger.annotations.ApiOperation
import io.swagger.annotations.ApiResponse
import io.swagger.annotations.ApiResponses
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.util.CollectionUtils
import org.springframework.web.bind.annotation.DeleteMapping
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.PutMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import javax.servlet.http.HttpServletRequest
import javax.validation.Valid

@RestController
@Api(description = "Api for managing disabled functionality rules")
@RequestMapping("/disabled/functionality")
class DisabledFunctionalityRulesController {

    @Autowired
    private lateinit var disabledFunctionalityRulesService: DisabledFunctionalityRulesService

    @PostMapping(consumes = [MediaType.APPLICATION_JSON_VALUE])
    @ApiOperation("Create disable functionality rule")
    @ApiResponses(
            ApiResponse(code = 200, message = "Success"),
            ApiResponse(code = 400, message = "Supplied rule is invalid"),
            ApiResponse(code = 500, message = "Internal server error occurred")
    )
    fun create(@Valid
               @RequestBody
               disabledFunctionalityRuleDto: DisabledFunctionalityRuleDto) {
        disabledFunctionalityRulesService.create(disabledFunctionalityRuleDto)
    }

    @PutMapping("/{id}", consumes = [MediaType.APPLICATION_JSON_VALUE])
    @ApiOperation("Update disable functionality rule")
    @ApiResponses(
            ApiResponse(code = 200, message = "Success"),
            ApiResponse(code = 400, message = "Supplied rule is invalid"),
            ApiResponse(code = 500, message = "Internal server error occurred")
    )
    fun update(@PathVariable("id") id: String,
               @Valid
               @RequestBody
               disabledFunctionalityRuleDto: DisabledFunctionalityRuleDto) {
        disabledFunctionalityRulesService.update(id, disabledFunctionalityRuleDto)
    }


    @GetMapping("/all", produces = [MediaType.APPLICATION_JSON_VALUE])
    @ApiOperation("Get all disable functionality rules")
    @ApiResponses(
            ApiResponse(code = 200, message = "Success"),
            ApiResponse(code = 500, message = "Internal server error occurred")
    )
    fun getAll(@RequestParam("enabled", required = false) enabled: Boolean? = null): List<DisabledFunctionalityRuleDto> {
        return disabledFunctionalityRulesService.getAll(enabled)
    }

    @GetMapping("/{id}", produces = [MediaType.APPLICATION_JSON_VALUE])
    @ApiOperation("Get disabled functionality rule")
    @ApiResponses(
            ApiResponse(code = 200, message = "Success"),
            ApiResponse(code = 500, message = "Internal server error occurred")
    )
    fun get(@PathVariable("id") id: String, @RequestParam("enabled", required = false) enabled: Boolean? = null): DisabledFunctionalityRuleDto? {
        return disabledFunctionalityRulesService.get(id, enabled)
    }

    @GetMapping("/history/{id}", produces = [MediaType.APPLICATION_JSON_VALUE])
    @ApiOperation("Gat modification history of disable functionality rule")
    @ApiResponses(
            ApiResponse(code = 200, message = "Success"),
            ApiResponse(code = 404, message = "History for supplied rule id is not found"),
            ApiResponse(code = 500, message = "Internal server error occurred")
    )
    fun history(@PathVariable("id") id: String): ResponseEntity<List<DisabledFunctionalityRuleDto>> {
        val historyRecords = disabledFunctionalityRulesService.history(id)
        if (CollectionUtils.isEmpty(historyRecords)) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(null)
        }

        return ResponseEntity.status(HttpStatus.OK).body(historyRecords)
    }

    @DeleteMapping("/{id}")
    @ApiOperation("Delete disable functionality rule")
    @ApiResponses(
            ApiResponse(code = 200, message = "Success"),
            ApiResponse(code = 404, message = "Rule is not found"),
            ApiResponse(code = 500, message = "Internal server error occurred")
    )
    fun delete(@PathVariable("id") id: String,
               @RequestBody
               @Valid
               deleteSettingRequestDto: DeleteSettingRequestDto) {
        disabledFunctionalityRulesService.delete(id, deleteSettingRequestDto)
    }

    @ExceptionHandler
    private fun handleSettingNotFound(request: HttpServletRequest, exception: SettingNotFoundException): ResponseEntity<String> {
        return ResponseEntity(exception.message, HttpStatus.NOT_FOUND)
    }

    @ExceptionHandler
    private fun handleValidationException(request: HttpServletRequest, exception: ValidationException): ResponseEntity<*> {
        return ResponseEntity(exception.message, HttpStatus.BAD_REQUEST)
    }
}
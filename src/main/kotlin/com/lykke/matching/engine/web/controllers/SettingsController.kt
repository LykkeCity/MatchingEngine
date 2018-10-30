package com.lykke.matching.engine.web.controllers

import com.lykke.matching.engine.daos.setting.AvailableSettingGroup
import com.lykke.matching.engine.daos.setting.InvalidSettingGroupException
import com.lykke.matching.engine.services.ApplicationSettingsService
import com.lykke.matching.engine.services.validators.impl.ValidationException
import com.lykke.matching.engine.web.dto.DeleteSettingRequestDto
import com.lykke.matching.engine.web.dto.SettingDto
import com.lykke.matching.engine.web.dto.SettingsGroupDto
import io.swagger.annotations.Api
import io.swagger.annotations.ApiOperation
import io.swagger.annotations.ApiResponse
import io.swagger.annotations.ApiResponses
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.MethodArgumentNotValidException
import org.springframework.web.bind.annotation.*
import java.util.stream.Collectors
import javax.servlet.http.HttpServletRequest
import javax.validation.Valid

@RestController
@RequestMapping("/settingsGroup")
@Api(description = "Api to manage ME application settings")
class SettingsController {

    private companion object {
        private val ERRORS_SEPARATOR = "; "
    }

    @Autowired
    private lateinit var applicationSettingsService: ApplicationSettingsService

    @ApiOperation("Get all settings of given group")
    @ApiResponses(
            ApiResponse(code = 200, message = "Success"),
            ApiResponse(code = 400, message = "Supplied group name is not supported"),
            ApiResponse(code = 500, message = "Internal server error occurred")
    )
    @GetMapping("/{settingGroupName}", produces = [MediaType.APPLICATION_JSON_VALUE])
    fun getSettingGroup(@PathVariable("settingGroupName") settingGroupName: String, @RequestParam("enabled", required = false) enabled: Boolean? = null): ResponseEntity<SettingsGroupDto> {
        val settingsGroup = applicationSettingsService.getSettingsGroup(AvailableSettingGroup.getBySettingsGroupName(settingGroupName), enabled)

        return if (settingsGroup != null) {
            ResponseEntity.ok(settingsGroup)
        } else {
            ResponseEntity.status(HttpStatus.NOT_FOUND).body(null)
        }
    }

    @ApiOperation("Get all settings of all setting groups")
    @ApiResponses(
            ApiResponse(code = 200, message = "Success"),
            ApiResponse(code = 500, message = "Internal server error occurred")
    )
    @GetMapping("/all", produces = [MediaType.APPLICATION_JSON_VALUE])
    fun getAllSettingGroups(@RequestParam("enabled", required = false) enabled: Boolean? = null): Set<SettingsGroupDto> {
        return applicationSettingsService.getAllSettingGroups(enabled)
    }

    @ApiOperation("Get setting for given setting group and setting name")
    @ApiResponses(
            ApiResponse(code = 200, message = "Success"),
            ApiResponse(code = 400, message = "Supplied group name is not supported"),
            ApiResponse(code = 404, message = "Supplied setting was not found"),
            ApiResponse(code = 500, message = "Internal server error occurred")
    )
    @GetMapping("/{settingGroupName}/setting/{settingName}", produces = [MediaType.APPLICATION_JSON_VALUE])
    fun getSetting(@PathVariable("settingGroupName") settingGroupName: String,
                   @PathVariable("settingName") settingName: String,
                   @RequestParam("enabled", required = false) enabled: Boolean? = null): ResponseEntity<SettingDto> {
        val setting = applicationSettingsService.getSetting(AvailableSettingGroup.getBySettingsGroupName(settingGroupName), settingName, enabled)

        return if (setting != null) {
            ResponseEntity.ok(setting)
        } else {
            ResponseEntity.status(HttpStatus.NOT_FOUND).body(null)
        }
    }

    @ApiOperation("Get list of supported setting groups")
    @ApiResponses(
            ApiResponse(code = 200, message = "Success"),
            ApiResponse(code = 500, message = "Internal server error occurred")
    )
    @GetMapping("/supported", produces = [MediaType.APPLICATION_JSON_VALUE])
    fun getSupportedSettings(): Set<String> {
        return AvailableSettingGroup.values().map { it.settingGroupName }.toSet()
    }

    @ApiOperation("Get history records for given setting")
    @ApiResponses(
            ApiResponse(code = 200, message = "Success"),
            ApiResponse(code = 400, message = "Supplied group name is not supported"),
            ApiResponse(code = 404, message = "Setting not found"),
            ApiResponse(code = 500, message = "Internal server error occurred")
    )
    @GetMapping("/{settingGroupName}/setting/{settingName}/history", produces = [MediaType.APPLICATION_JSON_VALUE])
    fun getHistoryRecords(@PathVariable("settingGroupName") settingGroupName: String,
                         @PathVariable("settingName") settingName: String): List<SettingDto> {
        return applicationSettingsService
                .getHistoryRecords(AvailableSettingGroup.getBySettingsGroupName(settingGroupName), settingName)
                .sortedByDescending { it.timestamp }
    }

    @ApiOperation("Create or update setting")
    @ApiResponses(
            ApiResponse(code = 200, message = "Success"),
            ApiResponse(code = 400, message = "Invalid setting was supplied"),
            ApiResponse(code = 500, message = "Internal server error occurred")
    )
    @PutMapping("/{settingGroupName}", consumes = [MediaType.APPLICATION_JSON_VALUE])
    fun createOrUpdate(@PathVariable("settingGroupName") settingGroupName: String,
                       @RequestBody
                       @Valid
                       settingDto: SettingDto) {
        applicationSettingsService.createOrUpdateSetting(AvailableSettingGroup.getBySettingsGroupName(settingGroupName), settingDto)
    }

    @ApiOperation("Delete all settings for given setting group")
    @ApiResponses(
            ApiResponse(code = 200, message = "Success"),
            ApiResponse(code = 400, message = "Supplied setting group is not supported"),
            ApiResponse(code = 500, message = "Internal server error occurred")
    )
    @DeleteMapping("/{settingGroupName}")
    fun deleteSettingsGroup(@PathVariable("settingGroupName") settingGroupName: String,
                            @RequestBody
                            @Valid
                            deleteRequest: DeleteSettingRequestDto) {
        applicationSettingsService.deleteSettingsGroup(AvailableSettingGroup.getBySettingsGroupName(settingGroupName), deleteRequest)
    }

    @ApiOperation("Delete setting for given setting group and given setting name")
    @ApiResponses(
            ApiResponse(code = 200, message = "Success"),
            ApiResponse(code = 400, message = "Supplied setting group is not supported"),
            ApiResponse(code = 404, message = "Setting not found"),
            ApiResponse(code = 500, message = "Internal server error occurred")
    )
    @DeleteMapping("/{settingGroupName}/setting/{settingName}")
    fun deleteSetting(@PathVariable("settingGroupName") settingGroupName: String,
                      @PathVariable("settingName") settingName: String,
                      @RequestBody
                      @Valid
                      deleteRequest: DeleteSettingRequestDto) {
        applicationSettingsService.deleteSetting(AvailableSettingGroup.getBySettingsGroupName(settingGroupName), settingName, deleteRequest)
    }

    @ExceptionHandler
    private fun handleApplicationValidationException(request: HttpServletRequest, exception: ValidationException): ResponseEntity<*> {
        return ResponseEntity(exception.message, HttpStatus.BAD_REQUEST)
    }

    @ExceptionHandler
    private fun handleValidationException(request: HttpServletRequest, exception: MethodArgumentNotValidException): ResponseEntity<String> {
        return ResponseEntity(exception
                .bindingResult
                .allErrors
                .stream()
                .map { it.defaultMessage }
                .collect(Collectors.joining(ERRORS_SEPARATOR)), HttpStatus.BAD_REQUEST)
    }

    @ExceptionHandler
    private fun handleInvalidSettingGroup(request: HttpServletRequest, exception: InvalidSettingGroupException): ResponseEntity<String> {
        return ResponseEntity(exception.message, HttpStatus.BAD_REQUEST)
    }
}
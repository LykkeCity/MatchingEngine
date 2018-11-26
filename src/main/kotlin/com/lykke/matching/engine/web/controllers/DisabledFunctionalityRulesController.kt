package com.lykke.matching.engine.web.controllers

import com.lykke.matching.engine.daos.DisableFunctionalityRule
import com.lykke.matching.engine.daos.setting.AvailableSettingGroup
import com.lykke.matching.engine.daos.setting.SettingNotFoundException
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.services.ApplicationSettingsService
import com.lykke.matching.engine.services.validators.impl.ValidationException
import com.lykke.matching.engine.services.validators.settings.impl.DisabledFunctionalitySettingValidator
import com.lykke.matching.engine.web.dto.DeleteSettingRequestDto
import com.lykke.matching.engine.web.dto.DisabledFunctionalityRuleDto
import com.lykke.matching.engine.web.dto.SettingDto
import io.swagger.annotations.Api
import io.swagger.annotations.ApiOperation
import io.swagger.annotations.ApiResponse
import io.swagger.annotations.ApiResponses
import org.nustaq.serialization.FSTConfiguration
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
import java.util.*
import javax.servlet.http.HttpServletRequest
import javax.validation.Valid

@RestController
@Api(description = "Api for managing disabled functionality rules")
@RequestMapping("/disabled/functionality")
class DisabledFunctionalityRulesController {

    private val conf = FSTConfiguration.createJsonConfiguration()

    @Autowired
    private lateinit var applicationSettingsService: ApplicationSettingsService

    @Autowired
    private lateinit var assetsPairsHolder: AssetsPairsHolder

    @Autowired
    private lateinit var assetsHolder: AssetsHolder

    @Autowired
    private lateinit var disabledFunctionalitySettingValidator: DisabledFunctionalitySettingValidator

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
        disabledFunctionalitySettingValidator.validate(disabledFunctionalityRuleDto)

        applicationSettingsService.createOrUpdateSetting(AvailableSettingGroup.DISABLED_FUNCTIONALITY_RULES,
                SettingDto(name = UUID.randomUUID().toString(),
                        value = conf.asJsonString(toDisabledFunctionalityRule(disabledFunctionalityRuleDto)),
                        enabled = disabledFunctionalityRuleDto.enabled,
                        comment = disabledFunctionalityRuleDto.comment,
                        user = disabledFunctionalityRuleDto.user))
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
        applicationSettingsService.createOrUpdateSetting(AvailableSettingGroup.DISABLED_FUNCTIONALITY_RULES,
                SettingDto(name = id,
                        value = conf.asJsonString(toDisabledFunctionalityRule(disabledFunctionalityRuleDto)),
                        enabled = disabledFunctionalityRuleDto.enabled,
                        comment = disabledFunctionalityRuleDto.comment,
                        user = disabledFunctionalityRuleDto.user))
    }


    @GetMapping("/all", produces = [MediaType.APPLICATION_JSON_VALUE])
    @ApiOperation("Get all disable functionality ")
    @ApiResponses(
            ApiResponse(code = 200, message = "Success"),
            ApiResponse(code = 500, message = "Internal server error occurred")
    )
    fun all(@RequestParam("enabled", required = false) enabled: Boolean? = null): List<DisabledFunctionalityRuleDto> {
        val result = ArrayList<DisabledFunctionalityRuleDto>()
        val settingsGroup = applicationSettingsService.getSettingsGroup(AvailableSettingGroup.DISABLED_FUNCTIONALITY_RULES, enabled)

        settingsGroup?.let {
            it.settings.forEach {
                result.add(toDisabledFunctionalityRuleDto(conf.asObject(it.value.toByteArray()) as DisableFunctionalityRule, it.name, it.timestamp, enabled))
            }
        }

        return result
    }

    @GetMapping("/{id}", produces = [MediaType.APPLICATION_JSON_VALUE])
    @ApiOperation("Get disabled functionality rule")
    @ApiResponses(
            ApiResponse(code = 200, message = "Success"),
            ApiResponse(code = 500, message = "Internal server error occurred")
    )
    fun get(@PathVariable("id") id: String, @RequestParam("enabled", required = false) enabled: Boolean? = null): DisabledFunctionalityRuleDto? {
        return applicationSettingsService.getSetting(AvailableSettingGroup.DISABLED_FUNCTIONALITY_RULES, id, enabled)?.let {
            toDisabledFunctionalityRuleDto(conf.asObject(it.value.toByteArray()) as DisableFunctionalityRule, it.name, it.timestamp, it.enabled)
        }
    }

    @GetMapping("/history/{id}", produces = [MediaType.APPLICATION_JSON_VALUE])
    @ApiOperation("History of modification of disabled functionality rule")
    @ApiResponses(
            ApiResponse(code = 200, message = "Success"),
            ApiResponse(code = 404, message = "History for supplied rule id is not found"),
            ApiResponse(code = 500, message = "Internal server error occurred")
    )
    fun history(@PathVariable("id") id: String): ResponseEntity<List<DisabledFunctionalityRuleDto>> {
        val historyRecords = applicationSettingsService.getHistoryRecords(AvailableSettingGroup.DISABLED_FUNCTIONALITY_RULES.settingGroupName, id)
        if (CollectionUtils.isEmpty(historyRecords)) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(null)
        }

        return ResponseEntity.ok(historyRecords
                .map {
                    toDisabledFunctionalityRuleDto(conf.asObject(it.value.toByteArray()) as DisableFunctionalityRule,
                            it.name,
                            it.timestamp,
                            it.enabled,
                            it.user,
                            it.comment)
                })
    }

    @DeleteMapping("/{id}")
    @ApiOperation("Delete disabled functionality rule")
    @ApiResponses(
            ApiResponse(code = 200, message = "Success"),
            ApiResponse(code = 404, message = "Rule was not found"),
            ApiResponse(code = 500, message = "Internal server error occurred")
    )
    fun delete(@PathVariable("id") id: String,
               @RequestBody
               @Valid
               deleteSettingRequestDto: DeleteSettingRequestDto) {

        applicationSettingsService.deleteSetting(AvailableSettingGroup.DISABLED_FUNCTIONALITY_RULES, id, deleteSettingRequestDto)
    }

    @ExceptionHandler
    private fun handleSettingNotFound(request: HttpServletRequest, exception: SettingNotFoundException): ResponseEntity<String> {
        return ResponseEntity(exception.message, HttpStatus.NOT_FOUND)
    }

    fun toDisabledFunctionalityRule(disabledFunctionalityRuleDto: DisabledFunctionalityRuleDto): DisableFunctionalityRule {
        return disabledFunctionalityRuleDto.let { rule ->
            DisableFunctionalityRule(rule.assetId?.let { assetsHolder.getAsset(it) },
                    rule.assetPairId?.let { assetsPairsHolder.getAssetPair(it) },
                    rule.messageTypeId?.let { MessageType.valueOf(it.toByte()) })
        }
    }

    fun toDisabledFunctionalityRuleDto(rule: DisableFunctionalityRule,
                                       id: String?,
                                       timestamp: Date?,
                                       enabled: Boolean?,
                                       comment: String? = null,
                                       user: String? = null): DisabledFunctionalityRuleDto {
        return DisabledFunctionalityRuleDto(
                id = id,
                assetId = rule.asset?.assetId,
                assetPairId = rule.assetPair?.assetPairId,
                messageTypeId = rule.messageType!!.type.toInt(),
                enabled = enabled,
                timestamp = timestamp,
                comment = comment,
                user = user)
    }

    @ExceptionHandler
    private fun handleValidationException(request: HttpServletRequest, exception: ValidationException): ResponseEntity<*> {
        return ResponseEntity(exception.message, HttpStatus.BAD_REQUEST)
    }
}
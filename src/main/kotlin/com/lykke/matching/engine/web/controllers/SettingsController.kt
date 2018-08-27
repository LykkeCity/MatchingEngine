package com.lykke.matching.engine.web.controllers

import com.lykke.matching.engine.services.ApplicationSettingsService
import com.lykke.matching.engine.web.dto.SettingDto
import com.lykke.matching.engine.web.dto.SettingsGroupDto
import io.swagger.annotations.Api
import io.swagger.annotations.ApiOperation
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*

@RestController("/settingsGroup")
@Api("Api to manage ME application settings")
class SettingsController {

    @Autowired
    private lateinit var applicationSettingsService: ApplicationSettingsService

    @ApiOperation("Get all settings of given group")
    @GetMapping("/{settingGroupName}", produces = [MediaType.APPLICATION_JSON_VALUE])
    fun getSettingGroup(@PathVariable("settingGroupName") settingGroupName: String, @RequestParam("enabled", required = false) enabled: Boolean? = null): ResponseEntity<SettingsGroupDto> {
        val settingsGroup = applicationSettingsService.getSettingsGroup(settingGroupName, enabled)

        return if (settingsGroup != null) {
            ResponseEntity.ok(settingsGroup)
        } else {
            ResponseEntity.status(HttpStatus.NOT_FOUND).body(null)
        }
    }

    @ApiOperation("Get all settings of all setting groups")
    @GetMapping("/all", produces = [MediaType.APPLICATION_JSON_VALUE])
    fun getAllSettingGroups(@RequestParam("enabled", required = false) enabled: Boolean? = null): Set<SettingsGroupDto> {
        return applicationSettingsService.getAllSettingGroups(enabled)
    }

    @ApiOperation("Get setting for given setting group and setting name")
    @GetMapping("/{settingGroupName}/setting/{settingName}", produces = [MediaType.APPLICATION_JSON_VALUE])
    fun getSetting(@PathVariable("settingGroupName") settingGroupName: String,
                   @PathVariable("settingName") settingName: String,
                   @RequestParam("enabled", required = false) enabled: Boolean? =  null): ResponseEntity<SettingDto> {
        val setting = applicationSettingsService.getSetting(settingGroupName, settingName, enabled)

        return if (setting != null) {
            ResponseEntity.ok(setting)
        } else {
            ResponseEntity.status(HttpStatus.NOT_FOUND).body(null)
        }
    }

    @ApiOperation("Create or update setting")
    @PutMapping("/{settingGroupName}", consumes = [MediaType.APPLICATION_JSON_VALUE])
    fun createOrUpdate(@PathVariable("settingGroupName") settingGroupName: String, settingDto: SettingDto) {
        applicationSettingsService.createOrUpdateSetting(settingGroupName, settingDto)
    }

    @ApiOperation("Delete all settings for given setting group")
    @DeleteMapping("/{settingGroupName}")
    fun deleteSettingsGroup(@PathVariable("settingGroupName") settingGroupName: String) {
        applicationSettingsService.deleteSettingsGroup(settingGroupName)
    }

    @ApiOperation("Delete setting for given setting group and given setting name")
    @DeleteMapping("/{settingGroupName}/setting/{settingName}")
    fun deleteSetting(@PathVariable("settingGroupName") settingGroupName: String, @PathVariable("settingName") settingName: String) {
        applicationSettingsService.deleteSetting(settingGroupName, settingName)
    }
}
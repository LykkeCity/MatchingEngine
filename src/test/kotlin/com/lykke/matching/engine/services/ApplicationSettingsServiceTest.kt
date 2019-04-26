package com.lykke.matching.engine.services

import com.lykke.matching.engine.AbstractTest
import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.setting.AvailableSettingGroup
import com.lykke.matching.engine.daos.setting.SettingHistoryRecord
import com.lykke.matching.engine.database.SettingsHistoryDatabaseAccessor
import com.lykke.matching.engine.database.TestSettingsDatabaseAccessor
import com.lykke.matching.engine.notification.SettingsListener
import com.lykke.matching.engine.services.validators.impl.ValidationException
import com.lykke.matching.engine.web.dto.DeleteSettingRequestDto
import com.lykke.matching.engine.web.dto.SettingDto
import com.nhaarman.mockito_kotlin.argumentCaptor
import com.nhaarman.mockito_kotlin.times
import com.nhaarman.mockito_kotlin.verify
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.junit4.SpringRunner
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [(TestApplicationContext::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class ApplicationSettingsServiceTest : AbstractTest() {

    @Autowired
    private lateinit var applicationSettingsService: ApplicationSettingsService

    @Autowired
    private lateinit var testSettingsDatabaseAccessor: TestSettingsDatabaseAccessor

    @Autowired
    private lateinit var settingsHistoryDatabaseAccessor: SettingsHistoryDatabaseAccessor

    @Autowired
    private lateinit var settingsListener: SettingsListener

    @Test
    fun getAllSettingGroupsTest() {
        //given
        applicationSettingsCache.createOrUpdateSettingValue(AvailableSettingGroup.TRUSTED_CLIENTS, "testClient", "testClient", true)
        applicationSettingsCache.createOrUpdateSettingValue(AvailableSettingGroup.DISABLED_ASSETS, "BTC", "BTC", true)

        //when
        val allSettingGroups = applicationSettingsService.getAllSettingGroups()

        //then
        assertEquals(2, allSettingGroups.size)

        val trustedClients = allSettingGroups.find { it.groupName == AvailableSettingGroup.TRUSTED_CLIENTS.settingGroupName }
        assertNotNull(trustedClients)

        val disabledAssets = allSettingGroups.find { it.groupName == AvailableSettingGroup.DISABLED_ASSETS.settingGroupName }
        assertNotNull(disabledAssets)

        assertEquals("testClient", trustedClients!!.settings.first().value)
        assertEquals("BTC", disabledAssets!!.settings.first().value)
    }

    @Test
    fun getSettingsGroupTest() {
        //given
        applicationSettingsCache.createOrUpdateSettingValue(AvailableSettingGroup.TRUSTED_CLIENTS, "testClient", "testClient", true)

        //when
        val settingsGroup = applicationSettingsService.getSettingsGroup(AvailableSettingGroup.TRUSTED_CLIENTS)

        //then
        assertNotNull(settingsGroup)
        assertEquals(1, settingsGroup!!.settings.size)
        assertEquals("testClient", settingsGroup.settings.first().value)
    }

    @Test
    fun getSettingTest() {
        //given
        applicationSettingsCache.createOrUpdateSettingValue(AvailableSettingGroup.TRUSTED_CLIENTS, "settingName", "testClient", true)

        //when
        val setting = applicationSettingsService.getSetting(AvailableSettingGroup.TRUSTED_CLIENTS, "settingName")

        //then
        assertNotNull(setting)
        assertEquals("testClient", setting!!.value)
    }

    @Test
    fun createOrUpdateSettingTest() {
        //given
        applicationSettingsCache.createOrUpdateSettingValue(AvailableSettingGroup.TRUSTED_CLIENTS, "settingName", "testClient", true)

        //when
        applicationSettingsService.createOrUpdateSetting(AvailableSettingGroup.TRUSTED_CLIENTS, SettingDto("settingName", "test", true, "testComment", "testUser"))

        //then
        val dbSetting = testSettingsDatabaseAccessor.getSetting(AvailableSettingGroup.TRUSTED_CLIENTS, "settingName")
        assertNotNull(dbSetting)
        assertEquals("test", dbSetting!!.value)

        assertTrue(applicationSettingsHolder.isTrustedClient("test"))
        assertEquals(1, settingsListener.getSettingChangeSize())

        argumentCaptor<SettingHistoryRecord>().apply {
            verify(settingsHistoryDatabaseAccessor).save(capture())
            assertEquals(AvailableSettingGroup.TRUSTED_CLIENTS.settingGroupName, firstValue.settingGroupName)
            assertEquals("settingName", firstValue.name)
            assertEquals("test", firstValue.value)
            assertEquals("testUser", firstValue.user)
            assertEquals("[UPDATE] testComment", firstValue.comment)
        }
    }

    @Test
    fun deleteSettingsGroupTest() {
        //given
        applicationSettingsCache.createOrUpdateSettingValue(AvailableSettingGroup.TRUSTED_CLIENTS, "settingName1", "testClient1", true)
        applicationSettingsCache.createOrUpdateSettingValue(AvailableSettingGroup.TRUSTED_CLIENTS, "settingName2", "testClient2", true)

        //when
        applicationSettingsService.deleteSettingsGroup(AvailableSettingGroup.TRUSTED_CLIENTS, DeleteSettingRequestDto("delete", "testUser"))

        //then
        val dbSetting = testSettingsDatabaseAccessor.getSetting(AvailableSettingGroup.TRUSTED_CLIENTS, "settingName")
        assertNull(dbSetting)
        assertEquals(1, settingsListener.getDeleteGroupSize())

        assertFalse(applicationSettingsHolder.isTrustedClient("testClient"))

        argumentCaptor<SettingHistoryRecord>().apply {
            verify(settingsHistoryDatabaseAccessor, times(2)).save(capture())
            val values = allValues.sortedBy { it.name }
            assertEquals("settingName1", values.get(0).name)
            assertEquals("testClient1", values.get(0).value)
            assertEquals("testUser", values.get(0).user)
            assertEquals("[DELETE] delete", values.get(0).comment)

            assertEquals("settingName2", values.get(1).name)
            assertEquals("testClient2", values.get(1).value)
            assertEquals("testUser", values.get(1).user)
            assertEquals("[DELETE] delete", values.get(1).comment)
        }
    }

    @Test
    fun deleteSettingTest() {
        //given
        applicationSettingsCache.createOrUpdateSettingValue(AvailableSettingGroup.TRUSTED_CLIENTS, "settingName", "testClient", true)

        //when
        applicationSettingsService.deleteSetting(AvailableSettingGroup.TRUSTED_CLIENTS, "settingName", DeleteSettingRequestDto("delete", "testUser"))

        //then
        val dbSetting = testSettingsDatabaseAccessor.getSetting(AvailableSettingGroup.TRUSTED_CLIENTS, "settingName")
        assertNull(dbSetting)
        assertEquals(1, settingsListener.getDeleteSize())

        assertFalse(applicationSettingsHolder.isTrustedClient("testClient"))

        argumentCaptor<SettingHistoryRecord>().apply {
            verify(settingsHistoryDatabaseAccessor).save(capture())

            assertEquals(AvailableSettingGroup.TRUSTED_CLIENTS.settingGroupName, firstValue.settingGroupName)
            assertEquals("settingName", firstValue.name)
            assertEquals("testClient", firstValue.value)
            assertEquals("testUser", firstValue.user)
            assertEquals("[DELETE] delete", firstValue.comment)
        }
    }

    @Test(expected = ValidationException::class)
    fun messageProcessingFlagValidationFailedTest() {
        //given
        testSettingsDatabaseAccessor.clear()

        //when
        applicationSettingsService
                .createOrUpdateSetting(AvailableSettingGroup.MESSAGE_PROCESSING_SWITCH,
                        SettingDto("test", "test", false, "testComment", "testUser"))
    }

    @Test
    fun messageProcessingFlagValidationPassedTest() {
        //given
        testSettingsDatabaseAccessor.clear()

        //when
        applicationSettingsService
                .createOrUpdateSetting(AvailableSettingGroup.MESSAGE_PROCESSING_SWITCH,
                        SettingDto("stop", "stop", true, "testComment", "testUser"))

        //then
        assertFalse(applicationSettingsHolder.isMessageProcessingEnabled())
        assertEquals(1, settingsListener.getSettingChangeSize())

        argumentCaptor<SettingHistoryRecord>().apply {
            verify(settingsHistoryDatabaseAccessor).save(capture())
            assertEquals(AvailableSettingGroup.MESSAGE_PROCESSING_SWITCH.settingGroupName, firstValue.settingGroupName)
            assertEquals("stop", firstValue.name)
            assertEquals("stop", firstValue.value)
            assertEquals("testUser", firstValue.user)
            assertEquals("[CREATE] testComment", firstValue.comment)
        }
    }
}
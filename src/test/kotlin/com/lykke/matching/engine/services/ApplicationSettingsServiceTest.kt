package com.lykke.matching.engine.services

import com.lykke.matching.engine.AbstractTest
import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.setting.AvailableSettingGroup
import com.lykke.matching.engine.daos.setting.SettingHistoryRecord
import com.lykke.matching.engine.database.SettingsHistoryDatabaseAccessor
import com.lykke.matching.engine.database.TestSettingsDatabaseAccessor
import com.lykke.matching.engine.notification.SettingsListener
import com.lykke.matching.engine.utils.getSetting
import com.lykke.matching.engine.web.dto.DeleteSettingRequestDto
import com.lykke.matching.engine.web.dto.SettingDto
import com.nhaarman.mockito_kotlin.argumentCaptor
import com.nhaarman.mockito_kotlin.times
import com.nhaarman.mockito_kotlin.verify
import junit.framework.Assert.*
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.junit4.SpringRunner
import kotlin.test.assertNotNull

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [(TestApplicationContext::class), (BalanceUpdateServiceTest.Config::class)])
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
        testSettingsDatabaseAccessor.clear()
        testSettingsDatabaseAccessor.createOrUpdateSetting(AvailableSettingGroup.TRUSTED_CLIENTS, getSetting("testClient"))
        testSettingsDatabaseAccessor.createOrUpdateSetting(AvailableSettingGroup.DISABLED_ASSETS, getSetting("BTC"))

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
        testSettingsDatabaseAccessor.clear()
        testSettingsDatabaseAccessor.createOrUpdateSetting(AvailableSettingGroup.TRUSTED_CLIENTS, getSetting("testClient"))

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
        testSettingsDatabaseAccessor.clear()
        testSettingsDatabaseAccessor.createOrUpdateSetting(AvailableSettingGroup.TRUSTED_CLIENTS, getSetting("testClient", "settingName"))

        //when
        val setting = applicationSettingsService.getSetting(AvailableSettingGroup.TRUSTED_CLIENTS, "settingName")

        //then
        assertNotNull(setting)
        assertEquals("testClient", setting!!.value)
    }

    @Test
    fun createOrUpdateSettingTest() {
        //given
        testSettingsDatabaseAccessor.clear()
        testSettingsDatabaseAccessor.createOrUpdateSetting(AvailableSettingGroup.TRUSTED_CLIENTS, getSetting("testClient", "settingName"))
        settingsListener.clear()

        //when
        applicationSettingsService.createOrUpdateSetting(AvailableSettingGroup.TRUSTED_CLIENTS, SettingDto("settingName", "test", true, "testComment", "testUser"))

        //then
        val dbSetting = testSettingsDatabaseAccessor.getSetting(AvailableSettingGroup.TRUSTED_CLIENTS, "settingName")
        assertNotNull(dbSetting)
        assertEquals("test", dbSetting!!.value)

        assertTrue(applicationSettingsCache.isTrustedClient("test"))
        assertEquals(1, settingsListener.getSettingChangeSize())


        argumentCaptor<SettingHistoryRecord>().apply {
            verify(settingsHistoryDatabaseAccessor).save(capture())
            assertEquals(AvailableSettingGroup.TRUSTED_CLIENTS, firstValue.settingGroup)
            assertEquals("settingName", firstValue.name)
            assertEquals("test", firstValue.value)
            assertEquals("testUser", firstValue.user)
            assertEquals("[UPDATE] testComment", firstValue.comment)
        }
    }

    @Test
    fun deleteSettingsGroupTest() {
        //given
        testSettingsDatabaseAccessor.clear()
        testSettingsDatabaseAccessor.createOrUpdateSetting(AvailableSettingGroup.TRUSTED_CLIENTS, getSetting("testClient1", "settingName1"))
        testSettingsDatabaseAccessor.createOrUpdateSetting(AvailableSettingGroup.TRUSTED_CLIENTS, getSetting("testClient2", "settingName2"))

        //when
        applicationSettingsService.deleteSettingsGroup(AvailableSettingGroup.TRUSTED_CLIENTS, DeleteSettingRequestDto("delete", "testUser"))

        //then
        val dbSetting = testSettingsDatabaseAccessor.getSetting(AvailableSettingGroup.TRUSTED_CLIENTS, "settingName")
        assertNull(dbSetting)
        assertEquals(1, settingsListener.getDeleteGroupSize())

        assertFalse(applicationSettingsCache.isTrustedClient("testClient"))

        argumentCaptor<SettingHistoryRecord>().apply {
            verify(settingsHistoryDatabaseAccessor, times(2)).save(capture())
            assertEquals(AvailableSettingGroup.TRUSTED_CLIENTS, firstValue.settingGroup)
            assertEquals("settingName1", firstValue.name)
            assertEquals("testClient1", firstValue.value)
            assertEquals("testUser", firstValue.user)
            assertEquals("[DELETE] delete", firstValue.comment)

            assertEquals(AvailableSettingGroup.TRUSTED_CLIENTS, secondValue.settingGroup)
            assertEquals("settingName2", secondValue.name)
            assertEquals("testClient2", secondValue.value)
            assertEquals("testUser", secondValue.user)
            assertEquals("[DELETE] delete", secondValue.comment)
        }
    }

    @Test
    fun deleteSettingTest() {
        //given
        testSettingsDatabaseAccessor.clear()
        testSettingsDatabaseAccessor.createOrUpdateSetting(AvailableSettingGroup.TRUSTED_CLIENTS, getSetting("testClient", "settingName"))

        //when
        applicationSettingsService.deleteSetting(AvailableSettingGroup.TRUSTED_CLIENTS, "settingName", DeleteSettingRequestDto("delete", "testUser"))

        //then
        val dbSetting = testSettingsDatabaseAccessor.getSetting(AvailableSettingGroup.TRUSTED_CLIENTS, "settingName")
        assertNull(dbSetting)
        assertEquals(1, settingsListener.getDeleteSize())

        assertFalse(applicationSettingsCache.isTrustedClient("testClient"))

        argumentCaptor<SettingHistoryRecord>().apply {
            verify(settingsHistoryDatabaseAccessor).save( capture())

            assertEquals(AvailableSettingGroup.TRUSTED_CLIENTS, firstValue.settingGroup)
            assertEquals("settingName", firstValue.name)
            assertEquals("testClient", firstValue.value)
            assertEquals("testUser", firstValue.user)
            assertEquals("[DELETE] delete", firstValue.comment)
        }
    }
}
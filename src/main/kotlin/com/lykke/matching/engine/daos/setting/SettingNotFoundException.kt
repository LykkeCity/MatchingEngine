package com.lykke.matching.engine.daos.setting

import com.lykke.matching.engine.exception.MatchingEngineException

class SettingNotFoundException(val settingName: String): MatchingEngineException("Setting with name '$settingName' is not found")
package com.lykke.matching.engine.daos.fee

import com.lykke.matching.engine.daos.FeeInstruction
import com.lykke.matching.engine.daos.FeeTransfer

data class Fee(val instruction: FeeInstruction,
               val transfer: FeeTransfer?)
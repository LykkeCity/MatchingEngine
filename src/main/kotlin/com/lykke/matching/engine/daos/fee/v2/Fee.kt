package com.lykke.matching.engine.daos.fee.v2

import com.lykke.matching.engine.daos.v2.FeeInstruction
import com.lykke.matching.engine.daos.FeeTransfer

data class Fee(val instruction: FeeInstruction,
               val transfer: FeeTransfer?)
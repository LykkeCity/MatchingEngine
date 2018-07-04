package com.lykke.matching.engine.outgoing.messages.v2

import com.lykke.matching.engine.messages.outgoing.OutgoingMessages

class FeeInstruction(val type: FeeType,
                     val size: String?,
                     val sizeType: FeeSizeType?,
                     val makerSize: String?,
                     val makerSizeType: FeeSizeType?,
                     val sourceWalletId: String?,
                     val targetWalletId: String?,
                     val assetsIds: List<String>?,
                     val makerFeeModificator: String?,
                     val index: Int) : AbstractEventPart<OutgoingMessages.FeeInstruction.Builder> {

    override fun createGeneratedMessageBuilder(): OutgoingMessages.FeeInstruction.Builder {
        val builder = OutgoingMessages.FeeInstruction.newBuilder()
        builder.type = type.id
        size?.let {
            builder.setSize(it)
        }
        sizeType?.let {
            builder.sizeType = it.id
        }
        makerSize?.let {
            builder.makerSize = it
        }
        makerSizeType?.let {
            builder.makerSizeType = it.id
        }
        sourceWalletId?.let {
            builder.sourceWalletId = it
        }
        targetWalletId?.let {
            builder.targetWalletId = it
        }
        assetsIds?.let {
            builder.addAllAssetsIds(it)
        }
        makerFeeModificator?.let {
            builder.makerFeeModificator = it
        }
        builder.index = index
        return builder
    }

}
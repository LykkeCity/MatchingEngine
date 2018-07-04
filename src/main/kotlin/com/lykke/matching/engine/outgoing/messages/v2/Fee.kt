package com.lykke.matching.engine.outgoing.messages.v2

import com.lykke.matching.engine.messages.outgoing.OutgoingMessages

class Fee(val instruction: FeeInstruction,
          val transfer: FeeTransfer?) : AbstractEventPart<OutgoingMessages.Fee.Builder> {

    override fun createGeneratedMessageBuilder(): OutgoingMessages.Fee.Builder {
        val builder = OutgoingMessages.Fee.newBuilder()
        builder.setInstruction(instruction.createGeneratedMessageBuilder())
        transfer?.let {
            builder.setTransfer(it.createGeneratedMessageBuilder())
        }
        return builder
    }

}
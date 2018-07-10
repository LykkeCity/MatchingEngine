package com.lykke.matching.engine.incoming.preprocessor.impl

import com.lykke.matching.engine.incoming.preprocessor.MessagePreprocessor
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageWrapper
import java.util.concurrent.BlockingQueue
import javax.annotation .PostConstruct
import kotlin.concurrent.thread

class DefaultPreprocessor(private val incomingQueue: BlockingQueue<MessageWrapper>,
                                                     private val outgoingQueue: BlockingQueue<MessageWrapper>) : MessagePreprocessor {
    override fun parseMessage(messageWrapper: MessageWrapper) {
        //do nothing
    }


    override fun preProcess(messageWrapper: MessageWrapper) {

    }


    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus) {

    }

        @PostConstruct

    fun init() {
        thread(start = true, name = DefaultPreprocessor::class.java.name) {
            while (true) {
                preProcess(incomingQueue.take())

            }

        }

    }

}
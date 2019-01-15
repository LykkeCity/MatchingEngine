package com.lykke.matching.engine.config.spring

import com.lykke.matching.engine.config.ConfigFactory
import com.lykke.matching.engine.outgoing.messages.v2.events.Event
import com.lykke.matching.engine.outgoing.messages.v2.events.ExecutionEvent
import com.lykke.matching.engine.outgoing.rabbit.utils.RabbitEventUtils
import org.springframework.beans.factory.config.BeanFactoryPostProcessor
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory
import org.springframework.beans.factory.support.AutowireCandidateQualifier
import org.springframework.beans.factory.support.DefaultListableBeanFactory
import org.springframework.beans.factory.support.RootBeanDefinition
import org.springframework.context.EnvironmentAware
import org.springframework.context.annotation.Configuration
import org.springframework.core.ResolvableType
import org.springframework.core.env.Environment
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue

@Configuration("dynamicRabbitMqQueueConfig")
open class DynamicRabbitMqQueueConfig : BeanFactoryPostProcessor, EnvironmentAware {
    private lateinit var environment: Environment

    private var clientEvents: BlockingQueue<Event<*>>? = null
    private var trustedEvents: BlockingQueue<ExecutionEvent>? = null

    override fun setEnvironment(environment: Environment) {
        this.environment = environment
    }

    override fun postProcessBeanFactory(beanFactory: ConfigurableListableBeanFactory) {
        registerClientEventsQueue(beanFactory as DefaultListableBeanFactory)
        registerTrustedClientsEventsQueue(beanFactory)
    }


    @RabbitQueue
    private fun registerClientEventsQueue(factory: DefaultListableBeanFactory) {
        val annotations = DynamicRabbitMqQueueConfig::class.java.getDeclaredMethod(Throwable().stackTrace[0].methodName, DefaultListableBeanFactory::class.java)
                .annotations
                .map { it.annotationClass.java }

        val config = ConfigFactory.getConfig(environment)

        val queueBeanDefinition = RootBeanDefinition(LinkedBlockingQueue::class.java)
        queueBeanDefinition.setTargetType(ResolvableType.forField(DynamicRabbitMqQueueConfig::class.java.getDeclaredField("clientEvents")))

        annotations.forEach {
            queueBeanDefinition.addQualifier(AutowireCandidateQualifier(it))
        }

        config.me.rabbitMqConfigs.events.forEachIndexed { index, eventConfig ->
            factory.registerBeanDefinition(RabbitEventUtils.getClientEventConsumerQueueName(eventConfig.exchange, index), queueBeanDefinition)
        }
    }

    @RabbitQueue
    private fun registerTrustedClientsEventsQueue(factory: DefaultListableBeanFactory) {

        val annotations = DynamicRabbitMqQueueConfig::class.java.getDeclaredMethod(Throwable().stackTrace[0].methodName, DefaultListableBeanFactory::class.java)
                .annotations
                .map { it.annotationClass.java }

        val config = ConfigFactory.getConfig(environment)

        val queueBeanDefinition = RootBeanDefinition(LinkedBlockingQueue::class.java)
        queueBeanDefinition.setTargetType(ResolvableType.forField(DynamicRabbitMqQueueConfig::class.java.getDeclaredField("trustedEvents")))

        annotations.forEach {
            queueBeanDefinition.addQualifier(AutowireCandidateQualifier(it))
        }

        config.me.rabbitMqConfigs.trustedClientsEvents.forEachIndexed { index, eventConfig ->
            factory.registerBeanDefinition(RabbitEventUtils.getTrustedClientsEventConsumerQueueName(eventConfig.exchange, index), queueBeanDefinition)
        }
    }
}
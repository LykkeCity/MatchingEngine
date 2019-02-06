package com.lykke.matching.engine.config.spring

import com.lykke.matching.engine.config.ConfigFactory
import com.lykke.matching.engine.logging.MessageWrapper
import com.lykke.matching.engine.outgoing.messages.v2.events.Event
import com.lykke.matching.engine.outgoing.messages.v2.events.ExecutionEvent
import com.lykke.matching.engine.outgoing.rabbit.utils.RabbitEventUtils
import com.lykke.matching.engine.utils.config.RabbitConfig
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
    private var databaseLogQueue: BlockingQueue<MessageWrapper>? = null

    override fun setEnvironment(environment: Environment) {
        this.environment = environment
    }

    override fun postProcessBeanFactory(beanFactory: ConfigurableListableBeanFactory) {
        registerClientEventsQueue(beanFactory as DefaultListableBeanFactory)
        registerTrustedClientsEventsQueue(beanFactory)
        registerClientDatabaseLogQueues(beanFactory)
    }

    @RabbitQueue
    private fun registerClientEventsQueue(factory: DefaultListableBeanFactory) {
        val config = ConfigFactory.getConfig(environment)
        registerQueue(factory, config.me.rabbitMqConfigs.events, "clientEvents", RabbitEventUtils.Companion::getClientEventConsumerQueueName)
    }

    @RabbitQueue
    private fun registerTrustedClientsEventsQueue(factory: DefaultListableBeanFactory) {
        val config = ConfigFactory.getConfig(environment)
        registerQueue(factory, config.me.rabbitMqConfigs.trustedClientsEvents, "trustedEvents", RabbitEventUtils.Companion::getTrustedClientsEventConsumerQueueName)
    }

    @DataQueue
    private fun registerClientDatabaseLogQueues(factory: DefaultListableBeanFactory) {
        val config = ConfigFactory.getConfig(environment)
        registerQueue(factory, config.me.rabbitMqConfigs.events, "databaseLogQueue", RabbitEventUtils.Companion::getDatabaseLogQueueName)
    }

    private fun registerQueue(factory: DefaultListableBeanFactory,
                              configs: Set<RabbitConfig>,
                              typeFieldName: String,
                              queueNameStrategy: (exchangeName: String, index: Int)-> String) {
        val annotations = DynamicRabbitMqQueueConfig::class.java.getDeclaredMethod(Throwable().stackTrace[1].methodName, DefaultListableBeanFactory::class.java)
                .annotations
                .map { it.annotationClass.java }

        val queueBeanDefinition = RootBeanDefinition(LinkedBlockingQueue::class.java)
        queueBeanDefinition.setTargetType(ResolvableType.forField(DynamicRabbitMqQueueConfig::class.java.getDeclaredField(typeFieldName)))

        annotations.forEach {
            queueBeanDefinition.addQualifier(AutowireCandidateQualifier(it))
        }

        configs.forEachIndexed { index, eventConfig ->
            factory.registerBeanDefinition(queueNameStrategy.invoke(eventConfig.exchange, index), queueBeanDefinition)
        }
    }
}
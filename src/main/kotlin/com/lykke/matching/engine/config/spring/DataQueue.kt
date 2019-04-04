package com.lykke.matching.engine.config.spring

import org.springframework.beans.factory.annotation.Qualifier

@Target(AnnotationTarget.CLASS, AnnotationTarget.FUNCTION, AnnotationTarget.FIELD, AnnotationTarget.VALUE_PARAMETER)
@Retention(AnnotationRetention.RUNTIME)
@Qualifier
annotation class DataQueue
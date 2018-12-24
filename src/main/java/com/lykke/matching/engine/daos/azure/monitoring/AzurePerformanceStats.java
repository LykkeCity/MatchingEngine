package com.lykke.matching.engine.daos.azure.monitoring;

import com.lykke.matching.engine.daos.TypePerformanceStats;
import com.microsoft.azure.storage.table.TableServiceEntity;

public class AzurePerformanceStats extends TableServiceEntity {
    private String type;
    private String inputQueueTime;
    private String preProcessingTime;
    private String preProcessedMessageQueueTime;
    private String totalTime;
    private String processingTime;
    private String persistTime;
    private Long persistCount;
    private Long count;

    public AzurePerformanceStats() {
    }

    public AzurePerformanceStats(String partitionKey, String rowKey, TypePerformanceStats stats) {
        super(partitionKey, rowKey);
        this.type = stats.getType();
        this.inputQueueTime = stats.getInputQueueTime();
        this.preProcessingTime = stats.getPreProcessingTime();
        this.preProcessedMessageQueueTime = stats.getPreProcessedMessageQueueTime();
        this.totalTime = stats.getTotalTime();
        this.processingTime = stats.getProcessingTime();
        this.count = stats.getCount();
        this.persistTime = stats.getPersistTime();
        this.persistCount = stats.getPersistCount();
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getTotalTime() {
        return totalTime;
    }

    public void setTotalTime(String totalTime) {
        this.totalTime = totalTime;
    }

    public String getProcessingTime() {
        return processingTime;
    }

    public void setProcessingTime(String processingTime) {
        this.processingTime = processingTime;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public String getPersistTime() {
        return persistTime;
    }

    public Long getPersistCount() {
        return persistCount;
    }

    public void setPersistTime(String persistTime) {
        this.persistTime = persistTime;
    }

    public void setPersistCount(Long persistCount) {
        this.persistCount = persistCount;
    }

    public String getInputQueueTime() {
        return inputQueueTime;
    }

    public String getPreProcessingTime() {
        return preProcessingTime;
    }

    public String getPreProcessedMessageQueue() {
        return preProcessedMessageQueueTime;
    }

    public void setInputQueueTime(String inputQueueTime) {
        this.inputQueueTime = inputQueueTime;
    }

    public void setPreProcessingTime(String preProcessingTime) {
        this.preProcessingTime = preProcessingTime;
    }

    public void setPreProcessedMessageQueue(String preProcessedMessageQueue) {
        this.preProcessedMessageQueueTime = preProcessedMessageQueue;
    }
}
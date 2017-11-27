package com.lykke.matching.engine.daos.azure.monitoring;

import com.lykke.matching.engine.daos.monitoring.MonitoringResult;
import com.microsoft.azure.storage.table.TableServiceEntity;

public class AzureMonitoringResult extends TableServiceEntity {
    private Double vmCpuLoad;
    private Double totalCpuLoad;
    private Long totalMemory;
    private Long freeMemory;
    private Long totalSwap;
    private Long freeSwap;
    private int threadsCount;

    public AzureMonitoringResult() {
    }

    public AzureMonitoringResult(String partitionKey, String rowKey, MonitoringResult monitoringResult) {
        super(partitionKey, rowKey);
        this.vmCpuLoad = monitoringResult.getVmCpuLoad();
        this.totalCpuLoad = monitoringResult.getTotalCpuLoad();
        this.totalMemory = monitoringResult.getTotalMemory();
        this.freeMemory = monitoringResult.getFreeMemory();
        this.totalSwap = monitoringResult.getTotalSwap();
        this.freeSwap = monitoringResult.getFreeSwap();
        this.threadsCount = monitoringResult.getThreadsCount();
    }

    public Double getVmCpuLoad() {
        return vmCpuLoad;
    }

    public void setVmCpuLoad(Double vmCpuLoad) {
        this.vmCpuLoad = vmCpuLoad;
    }

    public Double getTotalCpuLoad() {
        return totalCpuLoad;
    }

    public void setTotalCpuLoad(Double totalCpuLoad) {
        this.totalCpuLoad = totalCpuLoad;
    }

    public Long getTotalMemory() {
        return totalMemory;
    }

    public void setTotalMemory(Long totalMemory) {
        this.totalMemory = totalMemory;
    }

    public Long getFreeMemory() {
        return freeMemory;
    }

    public void setFreeMemory(Long freeMemory) {
        this.freeMemory = freeMemory;
    }

    public Long getTotalSwap() {
        return totalSwap;
    }

    public void setTotalSwap(Long totalSwap) {
        this.totalSwap = totalSwap;
    }

    public Long getFreeSwap() {
        return freeSwap;
    }

    public void setFreeSwap(Long freeSwap) {
        this.freeSwap = freeSwap;
    }

    public int getThreadsCount() {
        return threadsCount;
    }

    public void setThreadsCount(int threadsCount) {
        this.threadsCount = threadsCount;
    }
}

package com.lykke.matching.engine.daos.azure;

import com.microsoft.azure.storage.table.TableServiceEntity;
import java.util.Date;

public class AzureKeepAliveUpdate extends TableServiceEntity {
    public static final String MONITORING = "Monitoring";
    public static final String MATCHING_ENGINE = "MatchingEngine";

    private Date dateTime;
    private String version;

    public AzureKeepAliveUpdate() {
        super(MONITORING, MATCHING_ENGINE);
    }

    public AzureKeepAliveUpdate(Date date, String version) {
        super(MONITORING, MATCHING_ENGINE);
        this.dateTime = date;
        this.version = version;
    }

    public Date getDateTime() {
        return dateTime;
    }

    public void setDateTime(Date dateTime) {
        this.dateTime = dateTime;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }
}
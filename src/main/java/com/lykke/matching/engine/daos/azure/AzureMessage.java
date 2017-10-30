package com.lykke.matching.engine.daos.azure;

import com.microsoft.azure.storage.table.TableServiceEntity;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class AzureMessage extends TableServiceEntity {

    private static final SimpleDateFormat DATE_FORMAT = createDateFormatter();

    private String msgPart1;
    private String msgPart2;
    private String msgPart3;
    private String msgPart4;
    private String msgPart5;
    private String msgPart6;

    public AzureMessage() {
    }

    public AzureMessage(String id, String type, String[] parts) {
        super(generatePartitionKey(), String.format("%s_%s", id, type));
        msgPart1 = parts.length > 0 ? parts[0] : null;
        msgPart2 = parts.length > 1 ? parts[1] : null;
        msgPart3 = parts.length > 2 ? parts[2] : null;
        msgPart4 = parts.length > 3 ? parts[3] : null;
        msgPart5 = parts.length > 4 ? parts[4] : null;
        msgPart6 = parts.length > 5 ? parts[5] : null;

    }

    private synchronized static String generatePartitionKey() {
        return DATE_FORMAT.format(new Date());
    }

    private static SimpleDateFormat createDateFormatter() {
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
        format.setTimeZone(TimeZone.getTimeZone("UTC"));
        return format;
    }

    public String getMsgPart1() {
        return msgPart1;
    }

    public void setMsgPart1(String msgPart1) {
        this.msgPart1 = msgPart1;
    }

    public String getMsgPart2() {
        return msgPart2;
    }

    public void setMsgPart2(String msgPart2) {
        this.msgPart2 = msgPart2;
    }

    public String getMsgPart3() {
        return msgPart3;
    }

    public void setMsgPart3(String msgPart3) {
        this.msgPart3 = msgPart3;
    }

    public String getMsgPart4() {
        return msgPart4;
    }

    public void setMsgPart4(String msgPart4) {
        this.msgPart4 = msgPart4;
    }

    public String getMsgPart5() {
        return msgPart5;
    }

    public void setMsgPart5(String msgPart5) {
        this.msgPart5 = msgPart5;
    }

    public String getMsgPart6() {
        return msgPart6;
    }

    public void setMsgPart6(String msgPart6) {
        this.msgPart6 = msgPart6;
    }

    @Override
    public String toString() {
        return "AzureMessage(" +
                "rowKey=" + rowKey +
                ", msgPart1=" + msgPart1 +
                ", msgPart2=" + msgPart2 +
                ", msgPart3=" + msgPart3 +
                ", msgPart4=" + msgPart4 +
                ", msgPart5=" + msgPart5 +
                ", msgPart6=" + msgPart6 +
                ')';
    }
}
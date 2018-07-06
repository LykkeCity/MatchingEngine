package com.lykke.matching.engine.daos.azure;

import com.microsoft.azure.storage.table.TableServiceEntity;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class AzureMessage extends TableServiceEntity {

    private static final SimpleDateFormat DATE_FORMAT_PARTITION_KEY = createDateFormatter("yyyyMMdd");
    private static final SimpleDateFormat DATE_FORMAT_ROW_KEY = createDateFormatter("yyyy-MM-dd HH:mm:ss");

    private Long sequenceNumber;
    private String messageId;
    private String requestId;
    private Date messageTimestamp;
    private String message;
    private String msgPart2;
    private String msgPart3;
    private String msgPart4;
    private String msgPart5;
    private String msgPart6;
    private String messageBlobName;

    public AzureMessage() {
    }

    private AzureMessage(Long sequenceNumber, String messageId, String requestId, Date timestamp) {
        super(generatePartitionKey(), String.format("%s_%s", DATE_FORMAT_ROW_KEY.format(timestamp), messageId));
        this.sequenceNumber = sequenceNumber;
        this.messageId = messageId;
        this.requestId = requestId;
        messageTimestamp = timestamp;
    }

    public AzureMessage(Long sequenceNumber, String messageId, String requestId, Date timestamp, String[] parts) {
        this(sequenceNumber, messageId, requestId, timestamp);
        message = parts.length > 0 ? parts[0] : null;
        msgPart2 = parts.length > 1 ? parts[1] : null;
        msgPart3 = parts.length > 2 ? parts[2] : null;
        msgPart4 = parts.length > 3 ? parts[3] : null;
        msgPart5 = parts.length > 4 ? parts[4] : null;
        msgPart6 = parts.length > 5 ? parts[5] : null;
    }

    public AzureMessage(Long sequenceNumber, String messageId, String requestId, Date timestamp, String blobName) {
        this(sequenceNumber, messageId, requestId, timestamp);
        messageBlobName = blobName;
    }

    private synchronized static String generatePartitionKey() {
        return DATE_FORMAT_PARTITION_KEY.format(new Date());
    }

    private static SimpleDateFormat createDateFormatter(String pattern) {
        SimpleDateFormat format = new SimpleDateFormat(pattern);
        format.setTimeZone(TimeZone.getTimeZone("UTC"));
        return format;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
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

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public Long getSequenceNumber() {
        return sequenceNumber;
    }

    public void setSequenceNumber(Long sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public Date getMessageTimestamp() {
        return messageTimestamp;
    }

    public void setMessageTimestamp(Date messageTimestamp) {
        this.messageTimestamp = messageTimestamp;
    }

    public String getMessageBlobName() {
        return messageBlobName;
    }

    public void setMessageBlobName(String messageBlobName) {
        this.messageBlobName = messageBlobName;
    }

    @Override
    public String toString() {
        return "AzureMessage(" +
                "rowKey=" + rowKey +
                ", sequenceNumber=" + sequenceNumber +
                ", messageId=" + messageId +
                ", requestId=" + requestId +
                ", messageTimestamp=" + messageTimestamp +
                ", message=" + message +
                ", msgPart2=" + msgPart2 +
                ", msgPart3=" + msgPart3 +
                ", msgPart4=" + msgPart4 +
                ", msgPart5=" + msgPart5 +
                ", msgPart6=" + msgPart6 +
                ", messageBlobName=" + messageBlobName +
                ')';
    }
}
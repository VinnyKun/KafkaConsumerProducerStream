package org.example.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Message {
    private String senderName;
    private String recepientName;
    private  String body;
    @JsonCreator
    public Message(@JsonProperty("senderName") String senderName, @JsonProperty("recepientName") String recepientName, @JsonProperty("body")  String body) {
        this.senderName = senderName;
        this.recepientName = recepientName;
        this.body = body;
    }

    @Override
    public String toString() {
        return "Message{" +
                "senderName='" + senderName + '\'' +
                ", recepientName='" + recepientName + '\'' +
                ", body='" + body + '\'' +
                '}';
    }

    public String getSenderName() {
        return senderName;
    }

    public void setSenderName(String senderName) {
        this.senderName = senderName;
    }

    public String getRecepientName() {
        return recepientName;
    }

    public void setRecepientName(String recepientName) {
        this.recepientName = recepientName;
    }

    public String getBody() {
        return body;
    }

    public void setBody(String body) {
        this.body = body;
    }
}

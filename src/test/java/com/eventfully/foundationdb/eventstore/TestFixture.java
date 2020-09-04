package com.eventfully.foundationdb.eventstore;

import org.apache.commons.lang3.StringUtils;

import java.util.UUID;

abstract class TestFixture {

    static NewStreamMessage createNewStreamMessage() {
        return createNewStreamMessage(null, null);
    }

    static NewStreamMessage createNewStreamMessage(String data, String metadata) {
        return createNewStreamMessage(UUID.randomUUID(), data, metadata);
    }

    static NewStreamMessage createNewStreamMessage(UUID eventId, String data, String metadata) {
        return createNewStreamMessage(eventId, "type", data, metadata);
    }

    static NewStreamMessage createNewStreamMessage(UUID eventId, String type,  String data, String metadata) {
        return new NewStreamMessage(eventId, type, data == null ? "data".getBytes() : data.getBytes(), metadata == null ? "metadata".getBytes() : metadata.getBytes());
    }

    static NewStreamMessage[] createNewStreamMessages(int... messageNumbers) {
        return createNewStreamMessages("\"data\"", messageNumbers);
    }

    static NewStreamMessage[] createNewStreamMessages(String jsonData, int[] messageNumbers) {
        NewStreamMessage[] newMessages = new NewStreamMessage[messageNumbers.length];
        for (int i = 0; i < messageNumbers.length; i++) {
            UUID id = UUID.fromString(StringUtils.leftPad("00000000-0000-0000-0000-" + String.valueOf(messageNumbers[i]), 12, "0"));
            newMessages[i] = createNewStreamMessage(id, "type", jsonData, "metadata");
        }
        return newMessages;
    }
}

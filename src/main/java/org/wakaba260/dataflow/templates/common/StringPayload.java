package org.wakaba260.dataflow.templates.common;

public class StringPayload implements Payload<String> {
    private final String message;

    private StringPayload(String message) {
        this.message = message;
    }

    @Override
    public String getPayload() {
        return message;
    }

    @Override
    public String getOriginalPayload() {
        return message;
    }

    public static StringPayload of(String message) {
        return new StringPayload(message);
    }
}

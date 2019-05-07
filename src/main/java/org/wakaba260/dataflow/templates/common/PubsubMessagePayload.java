package org.wakaba260.dataflow.templates.common;

import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;

import java.nio.charset.StandardCharsets;

public class PubsubMessagePayload implements Payload<PubsubMessage> {
    private final PubsubMessage message;

    private PubsubMessagePayload(PubsubMessage message) {
        this.message = message;
    }

    @Override
    public String getPayload() {
        return new String(message.getPayload(), StandardCharsets.UTF_8);
    }

    @Override
    public PubsubMessage getOriginalPayload() {
        return message;
    }

    public static PubsubMessagePayload of(PubsubMessage message) {
        return new PubsubMessagePayload(message);
    }
}

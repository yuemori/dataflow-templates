package org.wakaba260.dataflow.templates.common;

import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class PubsubMessagePayloadCoder extends CustomCoder<PubsubMessagePayload> {
    private static final PubsubMessagePayloadCoder INSTANCE = new PubsubMessagePayloadCoder();
    private final PubsubMessageWithAttributesCoder CODER = PubsubMessageWithAttributesCoder.of();

    private PubsubMessagePayloadCoder() {
    }

    @Override
    public void encode(PubsubMessagePayload value, OutputStream outStream) throws CoderException, IOException {
        CODER.encode(value.getOriginalPayload(), outStream);
    }

    @Override
    public PubsubMessagePayload decode(InputStream inStream) throws CoderException, IOException {
        return PubsubMessagePayload.of(CODER.decode(inStream));
    }

    public static PubsubMessagePayloadCoder of() {
        return INSTANCE;
    }
}

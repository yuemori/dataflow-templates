package org.wakaba260.dataflow.templates.common;

import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeParameter;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class StringPayloadCoder extends CustomCoder<StringPayload> {
    private static final StringPayloadCoder INSTANCE = new StringPayloadCoder();
    private final StringUtf8Coder CODER = StringUtf8Coder.of();

    @Override
    public void encode(StringPayload value, OutputStream outStream) throws CoderException, IOException {
        CODER.encode(value.getOriginalPayload(), outStream);
    }

    @Override
    public StringPayload decode(InputStream inStream) throws CoderException, IOException {
        return StringPayload.of(CODER.decode(inStream));
    }

    public static StringPayloadCoder of() {
        return INSTANCE;
    }
}

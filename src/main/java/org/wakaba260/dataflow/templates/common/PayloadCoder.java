package org.wakaba260.dataflow.templates.common;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class PayloadCoder<T extends Payload> extends CustomCoder<T> {
    Coder<T> coder;

    private PayloadCoder(Coder<T> coder) {
        this.coder = coder;
    }

    @Override
    public void encode(T value, OutputStream outStream) throws CoderException, IOException {
        coder.encode(value, outStream);
    }

    @Override
    public T decode(InputStream inStream) throws CoderException, IOException {
        return coder.decode(inStream);
    }

    public static <T extends Payload> PayloadCoder<T> of(Coder<T> coder) {
        return new PayloadCoder<>(coder);
    }
}

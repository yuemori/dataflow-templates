package org.wakaba260.dataflow.templates.common;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.repackaged.beam_sdks_java_core.net.bytebuddy.description.type.TypeDescription;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestinationCoder;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeParameter;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class RecordCoder<T extends Payload> extends CustomCoder<Record<T>> {
    private final Coder<T> originalPayloadCoder;
    private final Coder<String> STRING_CODER = StringUtf8Coder.of();
    private final NullableCoder<String> NULLABLE_STRING_CODER = NullableCoder.of(StringUtf8Coder.of());
    private final NullableCoder<TableDestination> TD_CODER = NullableCoder.of(TableDestinationCoder.of());
    private final NullableCoder<TableRow> TR_CODER = NullableCoder.of(TableRowJsonCoder.of());

    private RecordCoder(Coder<T> originalPayloadCoder) {
        this.originalPayloadCoder = originalPayloadCoder;
    }

    public static <T extends Payload> RecordCoder of(Coder<T> originalPayloadCoder) {
        return new RecordCoder<T>(originalPayloadCoder);
    }

    @Override
    public void encode(Record<T> value, OutputStream outStream) throws CoderException, IOException {
        this.originalPayloadCoder.encode(value.getOriginalPayload(), outStream);
        STRING_CODER.encode(value.getPayload(), outStream);
        TD_CODER.encode(value.getDestination(), outStream);
        TR_CODER.encode(value.getRow(), outStream);
        NULLABLE_STRING_CODER.encode(value.getErrorMessage(), outStream);
        NULLABLE_STRING_CODER.encode(value.getStackTrace(), outStream);
    }

    @Override
    public Record decode(InputStream inStream) throws CoderException, IOException {
        return Record.of(
            this.originalPayloadCoder.decode(inStream),
            STRING_CODER.decode(inStream),
            TD_CODER.decode(inStream),
            TR_CODER.decode(inStream),
            NULLABLE_STRING_CODER.decode(inStream),
            NULLABLE_STRING_CODER.decode(inStream));
    }

    @Override
    public TypeDescriptor<Record<T>> getEncodedTypeDescriptor() {
        return new TypeDescriptor<Record<T>>() {}
            .where(new TypeParameter<T>() {}, this.originalPayloadCoder.getEncodedTypeDescriptor());
    }
}

package org.wakaba260.dataflow.templates.common;

import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.teleport.coders.FailsafeElementCoder;
import com.google.cloud.teleport.templates.common.ErrorConverters;
import com.google.cloud.teleport.util.ResourceUtils;
import com.google.cloud.teleport.values.FailsafeElement;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.*;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.ValueInSingleWindow;

import java.io.IOException;

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.Method;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;

public class BigQueryInsertWithDeadLetter extends PTransform<PCollection<KV<TableDestination, TableRow>>, WriteResult> {
    static final FailsafeElementCoder<String, String> FAILSAFE_ELEMENT_CODER =
            FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());
    private final BigQueryInsertWithDeadLetterOptions options;

    public static FailsafeElementCoder getCoder() {
        return FAILSAFE_ELEMENT_CODER;
    }

    public interface BigQueryInsertWithDeadLetterOptions extends PipelineOptions {
        @Default.Enum("CREATE_NEVER")
        CreateDisposition getBigQueryCreateDisposition();
        void setBigQueryCreateDisposition(CreateDisposition createDisposition);

        @Default.Enum("WRITE_APPEND")
        WriteDisposition getBigQueryWriteDisposition();
        void setBigQueryWriteDisposition(WriteDisposition writeDisposition);

        @Default.Enum("DEFAULT")
        Method getBigQueryInsertMethod();
        void setBigQueryInsertMethod(Method method);

        @Validation.Required
        @Description(
            "The dead-letter table to output to within BigQuery in <project-id>:<dataset>.<table> "
                + "format. If it doesn't exist, it will be created during pipeline execution.")
        ValueProvider<String> getOutputDeadletterTable();

        void setOutputDeadletterTable(ValueProvider<String> value);
    }

    public BigQueryInsertWithDeadLetter(BigQueryInsertWithDeadLetterOptions options) {
        this.options = options;
    }

    @Override
    public WriteResult expand(PCollection<KV<TableDestination, TableRow>> input) {
        WriteResult writeResult=
            input
                .apply(
                    BigQueryIO.<KV<TableDestination, TableRow>>write()
                        .withoutValidation()
                        .withCreateDisposition(options.getBigQueryCreateDisposition())
                        .withWriteDisposition(options.getBigQueryWriteDisposition())
                        .withFormatFunction((SerializableFunction<KV<TableDestination, TableRow>, TableRow>) element -> element.getValue())
                        .withExtendedErrorInfo()
                        .withMethod(options.getBigQueryInsertMethod())
                        .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
                        .to((SerializableFunction<ValueInSingleWindow<KV<TableDestination, TableRow>>, TableDestination>) element -> element.getValue().getKey()));

        PCollection<FailsafeElement<String, String>> failedInserts =
            writeResult
                .getFailedInsertsWithErr()
                .apply("WrapInsertionErrors",
                    MapElements.into(FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor())
                        .via((BigQueryInsertError e) -> wrapBigQueryInsertError(e)))
                .setCoder(FAILSAFE_ELEMENT_CODER);

        return failedInserts.apply("WriteFailedRecords",
            ErrorConverters.WriteStringMessageErrors.newBuilder()
                .setErrorRecordsTable(options.getOutputDeadletterTable())
                .setErrorRecordsTableSchema(ResourceUtils.getDeadletterTableSchemaJson())
                .build());
    }

    protected static FailsafeElement<String, String> wrapBigQueryInsertError(BigQueryInsertError insertError) {
        FailsafeElement<String, String> failsafeElement;

        JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
        try {
            TableRow row = insertError.getRow();
            row.setFactory(JSON_FACTORY);
            failsafeElement = FailsafeElement.of(row.toString(), row.toString());
            failsafeElement.setErrorMessage(insertError.getError().toPrettyString());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return failsafeElement;
    }
}

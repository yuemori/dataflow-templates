package org.wakaba260.dataflow.templates;

import avro.shaded.com.google.common.collect.ImmutableList;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.teleport.coders.FailsafeElementCoder;
import com.google.cloud.teleport.templates.common.ErrorConverters;
import com.google.cloud.teleport.util.ResourceUtils;
import com.google.cloud.teleport.values.FailsafeElement;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.*;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.wakaba260.dataflow.templates.common.BigQueryConverters;
import org.wakaba260.dataflow.templates.common.BigQueryInsertWithDeadLetter;
import org.wakaba260.dataflow.templates.common.JavascriptUdfExecutor.FailsafeTableDestinationJavascriptUdf;

import java.nio.charset.StandardCharsets;

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.Method;
import static org.wakaba260.dataflow.templates.common.BigQueryInsertWithDeadLetter.BigQueryInsertWithDeadLetterOptions;
import static org.wakaba260.dataflow.templates.common.JavascriptUdfExecutor.FailsafeTableDestinationJavascriptUdf.TableDestinationUdfOptions;
import static org.wakaba260.dataflow.templates.common.JavascriptUdfExecutor.FailsafeTransformJavascriptUdf;
import static org.wakaba260.dataflow.templates.common.JavascriptUdfExecutor.FailsafeTransformJavascriptUdf.TransformUdfOptions;

public class TextIOToBigQueryDynamicDestination {
    final static FailsafeElementCoder<String, String> PAYLOAD_CODER =
            FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());

    final static FailsafeElementCoder<String, KV<TableDestination, String>> FAILSAFE_PAYLOAD_CODER =
        FailsafeElementCoder.of(StringUtf8Coder.of(), KvCoder.of(TableDestinationCoder.of(), StringUtf8Coder.of()));

    final static FailsafeElementCoder<String, KV<TableDestination, TableRow>> FAILSAFE_TABLE_ROW_CODER =
        FailsafeElementCoder.of(StringUtf8Coder.of(), KvCoder.of(TableDestinationCoder.of(), TableRowJsonCoder.of()));

    static final TupleTag<FailsafeElement<String, KV<TableDestination, String>>> DESTINATION_TABLE_UDF_OUT = new TupleTag<>();

    static final TupleTag<FailsafeElement<String, String>> DESTINATION_TABLE_DEADLETTER_OUT = new TupleTag<>();

    static final TupleTag<FailsafeElement<String, KV<TableDestination, String>>> TRANSFORM_UDF_OUT = new TupleTag<>();

    static final TupleTag<FailsafeElement<String, String>> TRANSFORM_UDF_DEADLETTER_OUT = new TupleTag<>();

    static final TupleTag<KV<TableDestination, TableRow>> JSON_TO_TABLE_ROW_TRANSFORM_OUT = new TupleTag<>();

    static final TupleTag<FailsafeElement<String, String>> JSON_TO_TABLE_ROW_TRANSFORM_DEADLETTER_OUT = new TupleTag<>();

    public interface Options extends PipelineOptions, TableDestinationUdfOptions, TransformUdfOptions, BigQueryInsertWithDeadLetterOptions {
        @Description("The GCS location of the text you'd like to process")
        ValueProvider<String> getInputFilePattern();

        void setInputFilePattern(ValueProvider<String> value);
    }

    public static void main(String[] args) {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

        run(options);
    }

    public static PipelineResult run(Options options) {
        Pipeline pipeline = Pipeline.create(options);

        options.setBigQueryInsertMethod(Method.FILE_LOADS);

        CoderRegistry coderRegistry = pipeline.getCoderRegistry();
        coderRegistry.registerCoderForType(PAYLOAD_CODER.getEncodedTypeDescriptor(), PAYLOAD_CODER);
        coderRegistry.registerCoderForType(FAILSAFE_PAYLOAD_CODER.getEncodedTypeDescriptor(), FAILSAFE_PAYLOAD_CODER);
        coderRegistry.registerCoderForType(FAILSAFE_TABLE_ROW_CODER.getEncodedTypeDescriptor(), FAILSAFE_TABLE_ROW_CODER);

        FailsafeElementCoder bigQueryInsertCoder = BigQueryInsertWithDeadLetter.getCoder();
        coderRegistry.registerCoderForType(bigQueryInsertCoder.getEncodedTypeDescriptor(), bigQueryInsertCoder);

        /*
         * Step #1: Read from source
         */
        PCollection<String> inputs =
            pipeline
                .apply("ReadFromSource", TextIO.read().from(options.getInputFilePattern()));

        /*
         * Step #2: Apply Udf
         */
        PCollectionTuple convertedTableRows =
            inputs
                .apply("ConvertMessageToTableRow", new StringToTableRow(options));

        /*
         * Step #2: Insert into BigQuery
         */
        convertedTableRows
            .get(JSON_TO_TABLE_ROW_TRANSFORM_OUT)
            .apply(new BigQueryInsertWithDeadLetter(options));

        PCollectionList.of(
            ImmutableList.of(
                convertedTableRows.get(TRANSFORM_UDF_DEADLETTER_OUT),
                convertedTableRows.get(DESTINATION_TABLE_DEADLETTER_OUT),
                convertedTableRows.get(JSON_TO_TABLE_ROW_TRANSFORM_DEADLETTER_OUT)))
            .apply("Flatten", Flatten.pCollections())
            .apply("WriteFailedRecords",
                ErrorConverters.WriteStringMessageErrors.newBuilder()
                    .setErrorRecordsTable(options.getOutputDeadletterTable())
                    .setErrorRecordsTableSchema(ResourceUtils.getDeadletterTableSchemaJson())
                    .build());

        PipelineResult result = pipeline.run();
        result.waitUntilFinish();

        return result;
    }

    static class StringToTableRow extends PTransform<PCollection<String>, PCollectionTuple> {
        private final Options options;

        StringToTableRow(Options options) { this.options = options; }

        @Override
        public PCollectionTuple expand(PCollection<String> input) {
            PCollectionTuple destinationTableUdfOut =
                input
                    .apply("MapToRecord",
                        MapElements.into(PAYLOAD_CODER.getEncodedTypeDescriptor())
                            .via((String message) -> FailsafeElement.of(message, message)))
                    .apply("DestinationTableUDF",
                        FailsafeTableDestinationJavascriptUdf.<String>newBuilder()
                            .setFileSystemPath(options.getJavascriptTextTableDestinationGcsPath())
                            .setFunctionName(options.getJavascriptTextTableDestinationFunctionName())
                            .setProjectName(options.getDestinationProject())
                            .setDatasetId(options.getDestinationDatasetId())
                            .setSuccessTag(DESTINATION_TABLE_UDF_OUT)
                            .setFailureTag(DESTINATION_TABLE_DEADLETTER_OUT)
                            .build());

            PCollectionTuple transformUdfOut =
                destinationTableUdfOut
                    .get(DESTINATION_TABLE_UDF_OUT)
                    .apply("InvokeUDF",
                        FailsafeTransformJavascriptUdf.<String>newBuilder()
                            .setFileSystemPath(options.getJavascriptTextTableDestinationGcsPath())
                            .setFunctionName(options.getJavascriptTextTransformFunctionName())
                            .setSuccessTag(TRANSFORM_UDF_OUT)
                            .setFailureTag(TRANSFORM_UDF_DEADLETTER_OUT)
                            .build());

            PCollectionTuple jsonToTableRowOut =
                destinationTableUdfOut
                    .get(DESTINATION_TABLE_UDF_OUT)
                    .apply("jsonToTableRow",
                        BigQueryConverters.JsonToTableRow.<String>newBuilder()
                            .setSuccessTag(JSON_TO_TABLE_ROW_TRANSFORM_OUT)
                            .setFailureTag(JSON_TO_TABLE_ROW_TRANSFORM_DEADLETTER_OUT)
                            .build());

             return PCollectionTuple.of(TRANSFORM_UDF_OUT, transformUdfOut.get(TRANSFORM_UDF_OUT))
                 .and(TRANSFORM_UDF_DEADLETTER_OUT, transformUdfOut.get(TRANSFORM_UDF_DEADLETTER_OUT))
                 .and(DESTINATION_TABLE_UDF_OUT, destinationTableUdfOut.get(DESTINATION_TABLE_UDF_OUT))
                 .and(DESTINATION_TABLE_DEADLETTER_OUT, destinationTableUdfOut.get(DESTINATION_TABLE_DEADLETTER_OUT))
                 .and(JSON_TO_TABLE_ROW_TRANSFORM_OUT, jsonToTableRowOut.get(JSON_TO_TABLE_ROW_TRANSFORM_OUT))
                 .and(JSON_TO_TABLE_ROW_TRANSFORM_DEADLETTER_OUT, jsonToTableRowOut.get(JSON_TO_TABLE_ROW_TRANSFORM_DEADLETTER_OUT));
        }
    }
}

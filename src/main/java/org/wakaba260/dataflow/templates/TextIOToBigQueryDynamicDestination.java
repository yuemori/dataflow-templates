package org.wakaba260.dataflow.templates;

import avro.shaded.com.google.common.collect.ImmutableList;
import com.google.cloud.teleport.coders.FailsafeElementCoder;
import com.google.cloud.teleport.templates.common.ErrorConverters;
import com.google.cloud.teleport.util.ResourceUtils;
import com.google.cloud.teleport.values.FailsafeElement;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.wakaba260.dataflow.templates.common.*;

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.Method;
import static org.wakaba260.dataflow.templates.common.BigQueryInsertWithDeadLetter.BigQueryInsertWithDeadLetterOptions;
import static org.wakaba260.dataflow.templates.common.JavascriptUdfExecutor.*;

/**
 * The {@link TextIOToBigQueryDynamicDestination} pipeline is a batch pipeline which ingests data in JSON format
 * from TextIO, executes a transform and table destination UDF, and outputs the resulting records to BigQuery. Any errors
 * which occur in the transformation of the data or execution of the UDF will be output to a
 * separate errors table in BigQuery. The errors table will be created if it does not exist prior to
 * execution. Both output and error tables are specified by the user as template parameters.
 *
 * <p><b>Pipeline Requirements</b>
 *
 * <ul>
 *   <li>The Pub/Sub topic exists.
 *   <li>The BigQuery output table exists.
 * </ul>
 *
 * <p><b>Example Usage</b>
 *
 * <pre>
 * # Set the pipeline vars
 * PROJECT_ID=PROJECT ID HERE
 * BUCKET_NAME=BUCKET NAME HERE
 * PIPELINE_FOLDER=gs://${BUCKET_NAME}/dataflow/pipelines/textio-to-bigquery-dynamic-destination
 * UDF_PATH=gs://${BUCKET_NAME}/dataflow/pipelines/textio-to-bigquery-dynamic-destination.js
 * USE_SUBSCRIPTION=true or false depending on whether the pipeline should read
 *                  from a Pub/Sub Subscription or a Pub/Sub Topic.
 *
 * # Set the runner
 * RUNNER=DataflowRunner
 *
 * # Build the template
 * mvn compile exec:java \
 * -Dexec.mainClass=org.wakaba260.dataflow.templates.TextIOToBigQueryDynamicDestination \
 * -Dexec.cleanupDaemonThreads=false \
 * -Dexec.args=" \
 * --project=${PROJECT_ID} \
 * --stagingLocation=${PIPELINE_FOLDER}/staging \
 * --tempLocation=${PIPELINE_FOLDER}/temp \
 * --templateLocation=${PIPELINE_FOLDER}/template \
 * --runner=${RUNNER} \
 * --bigQueryWriteDisposition=WRITE_APPEND"
 *
 * # Execute the template
 * JOB_NAME=textio-to-bigquery-$USER-`date +"%Y%m%d-%H%M%S%z"`
 *
 * # Execute a pipeline to read from a Topic.
 * gcloud dataflow jobs run ${JOB_NAME} \
 * --gcs-location=${PIPELINE_FOLDER}/template \
 * --zone=us-east1-d \
 * --parameters \
 * "inputFilePattern=gs://${BUCKET_NAME}/logs/*.json,\
 * outputDeadletterTable=${PROJECT_ID}:dataset-id.deadletter-table,\
 * javascriptUdfGcsPath=${UDF_PATH},\
 * javascriptUdfGetTableNameFunctionName=getDestinationTable,\
 * javascriptUdfTransformFunctionName=transform,\
 * dynamicDestinationProject=${PROJECT_ID},\
 * dynamicDestinationDatasetId=${DATASET_ID}"
 * </pre>
 */
public class TextIOToBigQueryDynamicDestination {
    final static Coder<StringPayload> PAYLOAD_CODER = StringPayloadCoder.of();
    final static RecordCoder<StringPayload> RECORD_CODER = RecordCoder.of(PAYLOAD_CODER);

    final static FailsafeElementCoder<String, String> FAILSAFE_ELEMENT_CODER =
        FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());

    static final TupleTag<Record<StringPayload>> DESTINATION_TABLE_UDF_OUT = new TupleTag<Record<StringPayload>>() {};

    static final TupleTag<Record<StringPayload>> DESTINATION_TABLE_DEADLETTER_OUT = new TupleTag<Record<StringPayload>>() {};

    static final TupleTag<Record<StringPayload>> TRANSFORM_UDF_OUT = new TupleTag<Record<StringPayload>>() {};

    static final TupleTag<Record<StringPayload>> TRANSFORM_UDF_DEADLETTER_OUT = new TupleTag<Record<StringPayload>>() {};

    static final TupleTag<Record<StringPayload>> JSON_TO_TABLE_ROW_TRANSFORM_OUT = new TupleTag<Record<StringPayload>>() {};

    static final TupleTag<Record<StringPayload>> JSON_TO_TABLE_ROW_TRANSFORM_DEADLETTER_OUT = new TupleTag<Record<StringPayload>>() {};

    public interface Options extends PipelineOptions, JavascriptUdfExecutorOptions, BigQueryInsertWithDeadLetterOptions {
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
        coderRegistry.registerCoderForType(RECORD_CODER.getEncodedTypeDescriptor(), RECORD_CODER);

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
                .apply("MapToPayload",
                    MapElements.into(PAYLOAD_CODER.getEncodedTypeDescriptor())
                        .via((String message) -> StringPayload.of(message)))
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
            .apply("WrapErrors",
                MapElements.into(FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor())
                    .via((Record record) -> FailsafeElement.of(record.getOriginalPayload().getPayload(), record.getPayload())))
            .apply("WriteFailedRecords",
                ErrorConverters.WriteStringMessageErrors.newBuilder()
                    .setErrorRecordsTable(options.getOutputDeadletterTable())
                    .setErrorRecordsTableSchema(ResourceUtils.getDeadletterTableSchemaJson())
                    .build());

        PipelineResult result = pipeline.run();
        result.waitUntilFinish();

        return result;
    }

    static class StringToTableRow extends PTransform<PCollection<StringPayload>, PCollectionTuple> {
        private final Options options;

        StringToTableRow(Options options) { this.options = options; }

        @Override
        public PCollectionTuple expand(PCollection<StringPayload> input) {
            PCollectionTuple destinationTableUdfOut =
                input
                    .apply("MapToRecord",
                        MapElements.into(RECORD_CODER.getEncodedTypeDescriptor())
                            .via(payload -> Record.of(payload)))
                    .apply("GetDestinationTableUDF",
                        RecordJavascriptGetTableDestinationUdf.<StringPayload>newBuilder()
                            .setFileSystemPath(options.getJavascriptUdfGcsPath())
                            .setFunctionName(options.getJavascriptUdfGetTableNameFunctionName())
                            .setProjectName(options.getDynamicDestinationProject())
                            .setDatasetId(options.getDynamicDestinationDatasetId())
                            .setSuccessTag(DESTINATION_TABLE_UDF_OUT)
                            .setFailureTag(DESTINATION_TABLE_DEADLETTER_OUT)
                            .build());

            PCollectionTuple transformUdfOut =
                destinationTableUdfOut
                    .get(DESTINATION_TABLE_UDF_OUT)
                    .apply("InvokeUDF",
                        RecordJavascriptTransformUdf.<StringPayload>newBuilder()
                            .setFileSystemPath(options.getJavascriptUdfGcsPath())
                            .setFunctionName(options.getJavascriptUdfTransformFunctionName())
                            .setSuccessTag(TRANSFORM_UDF_OUT)
                            .setFailureTag(TRANSFORM_UDF_DEADLETTER_OUT)
                            .build());

            PCollectionTuple jsonToTableRowOut =
                destinationTableUdfOut
                    .get(DESTINATION_TABLE_UDF_OUT)
                    .apply("jsonToTableRow",
                        BigQueryConverters.RecordToTableRow.<StringPayload>newBuilder()
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

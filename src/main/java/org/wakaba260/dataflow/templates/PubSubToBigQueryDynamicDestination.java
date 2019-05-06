package org.wakaba260.dataflow.templates;

import com.google.common.collect.ImmutableList;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.teleport.coders.FailsafeElementCoder;
import com.google.cloud.teleport.templates.common.ErrorConverters;
import com.google.cloud.teleport.util.ResourceUtils;
import com.google.cloud.teleport.values.FailsafeElement;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.Method;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestinationCoder;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.wakaba260.dataflow.templates.JavascriptUdfExecutor.FailsafeTransformJavascriptUdf;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.wakaba260.dataflow.templates.JavascriptUdfExecutor.FailsafeTableDestinationJavascriptUdf.TableDestinationUdfOptions;
import static org.wakaba260.dataflow.templates.JavascriptUdfExecutor.FailsafeTransformJavascriptUdf.TransformUdfOptions;

/**
 * The {@link PubSubToBigQueryDynamicDestination} pipeline is a streaming pipeline which ingests data in JSON format
 * from Cloud Pub/Sub, executes a trainsform and table destination UDF, and outputs the resulting records to BigQuery. Any errors
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
 * PIPELINE_FOLDER=gs://${BUCKET_NAME}/dataflow/pipelines/pubsub-to-bigquery-dynamic-destination
 * USE_SUBSCRIPTION=true or false depending on whether the pipeline should read
 *                  from a Pub/Sub Subscription or a Pub/Sub Topic.
 *
 * # Set the runner
 * RUNNER=DataflowRunner
 *
 * # Build the template
 * mvn compile exec:java \
 * -Dexec.mainClass=org.wakaba260.dataflow.templates.PubSubToBigQueryDynamicDestination \
 * -Dexec.cleanupDaemonThreads=false \
 * -Dexec.args=" \
 * --project=${PROJECT_ID} \
 * --stagingLocation=${PIPELINE_FOLDER}/staging \
 * --tempLocation=${PIPELINE_FOLDER}/temp \
 * --templateLocation=${PIPELINE_FOLDER}/template \
 * --runner=${RUNNER}
 * --useSubscription=${USE_SUBSCRIPTION}
 * "
 *
 * # Execute the template
 * JOB_NAME=pubsub-to-bigquery-$USER-`date +"%Y%m%d-%H%M%S%z"`
 *
 * # Execute a pipeline to read from a Topic.
 * gcloud dataflow jobs run ${JOB_NAME} \
 * --gcs-location=${PIPELINE_FOLDER}/template \
 * --zone=us-east1-d \
 * --parameters \
 * "inputTopic=projects/${PROJECT_ID}/topics/input-topic-name,\
 * outputTableSpec=${PROJECT_ID}:dataset-id.output-table,\
 * outputDeadletterTable=${PROJECT_ID}:dataset-id.deadletter-table"
 *
 * # Execute a pipeline to read from a Subscription.
 * gcloud dataflow jobs run ${JOB_NAME} \
 * --gcs-location=${PIPELINE_FOLDER}/template \
 * --zone=us-east1-d \
 * --parameters \
 * "inputSubscription=projects/${PROJECT_ID}/subscriptions/input-subscription-name,\
 * outputTableSpec=${PROJECT_ID}:dataset-id.output-table,\
 * outputDeadletterTable=${PROJECT_ID}:dataset-id.deadletter-table"
 * </pre>
 */
public class PubSubToBigQueryDynamicDestination {
    public interface Options extends DataflowPipelineOptions, TableDestinationUdfOptions, TransformUdfOptions {
        @Description(
            "The Cloud Pub/Sub subscription to consume from. "
                + "The name should be in the format of "
                + "projects/<project-id>/subscriptions/<subscription-name>."
        )
        ValueProvider<String> getInputSubscription();
        void setInputSubscription(ValueProvider<String> subscription);

        @Description("Pub/Sub topic to read the input from")
        ValueProvider<String> getInputTopic();

        void setInputTopic(ValueProvider<String> value);

        @Validation.Required
        @Description(
            "The dead-letter table to output to within BigQuery in <project-id>:<dataset>.<table> "
                + "format. If it doesn't exist, it will be created during pipeline execution.")
        ValueProvider<String> getOutputDeadletterTable();

        void setOutputDeadletterTable(ValueProvider<String> value);

        @Description(
            "This determines whether the template reads from " + "a pub/sub subscription or a topic")
        @Default.Boolean(false)
        Boolean getUseSubscription();
        void setUseSubscription(Boolean value);
    }

    final static FailsafeElementCoder<PubsubMessage, String> CODER =
        FailsafeElementCoder.of(PubsubMessageWithAttributesCoder.of(), StringUtf8Coder.of());

    final static FailsafeElementCoder<PubsubMessage, KV<TableDestination, String>> FAILSAFE_PAYLOAD_CODER =
        FailsafeElementCoder.of(PubsubMessageWithAttributesCoder.of(), KvCoder.of(TableDestinationCoder.of(), StringUtf8Coder.of()));

    final static FailsafeElementCoder<PubsubMessage, KV<TableDestination, TableRow>> FAILSAFE_TABLE_ROW_CODER =
        FailsafeElementCoder.of(PubsubMessageWithAttributesCoder.of(), KvCoder.of(TableDestinationCoder.of(), TableRowJsonCoder.of()));

    final static TupleTag<FailsafeElement<PubsubMessage, KV<TableDestination, String>>> TRANSFORM_UDF_OUT =
        new TupleTag<FailsafeElement<PubsubMessage, KV<TableDestination, String>>>() {};

    final static TupleTag<FailsafeElement<PubsubMessage, KV<TableDestination, String>>> DESTINATION_TABLE_UDF_OUT =
            new TupleTag<FailsafeElement<PubsubMessage, KV<TableDestination, String>>>() {};

    static final TupleTag<KV<TableDestination, TableRow>> JSON_TO_TABLE_ROW_TRANSFORM_OUT = new TupleTag<KV<TableDestination, TableRow>>() {};

    final static TupleTag<FailsafeElement<PubsubMessage, String>> TRANSFORM_UDF_DEADLETTER_OUT =
        new TupleTag<FailsafeElement<PubsubMessage, String>>() {};

    final static TupleTag<FailsafeElement<PubsubMessage, String>> DESTINATION_TABLE_DEADLETTER_OUT =
            new TupleTag<FailsafeElement<PubsubMessage, String>>() {};

    static final TupleTag<FailsafeElement<PubsubMessage, String>> JSON_TO_TABLE_ROW_TRANSFORM_DEADLETTER_OUT =
        new TupleTag<FailsafeElement<PubsubMessage, String>>() {};

    static final FailsafeElementCoder<String, String> FAILSAFE_ELEMENT_CODER =
        FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());

    public static void main(String[] args) {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

        run(options);
    }

    public static PipelineResult run(Options options) {
        Pipeline pipeline = Pipeline.create(options);

        PCollection<PubsubMessage> messages = null;

        CoderRegistry coderRegistry = pipeline.getCoderRegistry();
        coderRegistry.registerCoderForType(CODER.getEncodedTypeDescriptor(), CODER);
        coderRegistry.registerCoderForType(FAILSAFE_PAYLOAD_CODER.getEncodedTypeDescriptor(), FAILSAFE_PAYLOAD_CODER);
        coderRegistry.registerCoderForType(FAILSAFE_TABLE_ROW_CODER.getEncodedTypeDescriptor(), FAILSAFE_TABLE_ROW_CODER);

        /*
         * Step #1: Read messages from PubSub
         */
        if(options.getUseSubscription()) {
            messages = pipeline.apply("ReadPubSubSubscription",
                PubsubIO.readMessagesWithAttributes().fromSubscription(options.getInputSubscription()));
        } else {
            messages = pipeline.apply("ReadPubSubTopic",
                PubsubIO.readMessagesWithAttributes().fromTopic(options.getInputTopic()));
        }

        /*
         * Step #2: Create KV of DestinationTable and transformed String by UDF.
         */
        PCollectionTuple convertedTableRows =
            messages
                .apply("ConvertMessageToTableRow", new PubSubMessageToTableRow(options));

        /*
         * Step #3: Write the successful records to BigQuery.
         */
        WriteResult writeResult =
            convertedTableRows
                .get(JSON_TO_TABLE_ROW_TRANSFORM_OUT)
                .apply(
                    BigQueryIO.<KV<TableDestination, TableRow>>write()
                        .withoutValidation()
                        .withCreateDisposition(CreateDisposition.CREATE_NEVER)
                        .withWriteDisposition(Write.WriteDisposition.WRITE_APPEND)
                        .withFormatFunction((SerializableFunction<KV<TableDestination, TableRow>, TableRow>) element -> element.getValue())
                        .withExtendedErrorInfo()
                        .withMethod(Method.STREAMING_INSERTS)
                        .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
                        .to((SerializableFunction<ValueInSingleWindow<KV<TableDestination, TableRow>>, TableDestination>) element -> element.getValue().getKey()));

        /*
         * Step 3 Contd.
         * Elements that failed inserts into BigQuery are extracted and converted to FailsafeElement
         */
        PCollection<FailsafeElement<String, String>> failedInserts =
            writeResult
                .getFailedInsertsWithErr()
                .apply("WrapInsertionErrors",
                    MapElements.into(FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor())
                    .via((BigQueryInsertError e) -> wrapBigQueryInsertError(e)))
                .setCoder(FAILSAFE_ELEMENT_CODER);

        /*
         * Step #4: Write records that failed table row transformation
         * or conversion out to BigQuery deadletter table.
         */
        PCollectionList.of(
            ImmutableList.of(
                convertedTableRows.get(TRANSFORM_UDF_DEADLETTER_OUT),
                convertedTableRows.get(DESTINATION_TABLE_DEADLETTER_OUT),
                convertedTableRows.get(JSON_TO_TABLE_ROW_TRANSFORM_DEADLETTER_OUT)))
            .apply("Flatten", Flatten.pCollections())
            .apply("WriteFailedRecords",
                ErrorConverters.WritePubsubMessageErrors.newBuilder()
                    .setErrorRecordsTable( options.getOutputDeadletterTable())
                    .setErrorRecordsTableSchema(ResourceUtils.getDeadletterTableSchemaJson())
                    .build());

        /*
         * Step #5: Insert records that failed insert into deadletter table
         */
        failedInserts.apply("WriteFailedRecords",
            ErrorConverters.WriteStringMessageErrors.newBuilder()
                .setErrorRecordsTable(options.getOutputDeadletterTable())
                .setErrorRecordsTableSchema(ResourceUtils.getDeadletterTableSchemaJson())
                .build());

        return pipeline.run();
    }

    static class PubSubMessageToTableRow extends PTransform<PCollection<PubsubMessage>, PCollectionTuple> {
        private final Options options;

        PubSubMessageToTableRow(Options options) {
            this.options = options;
        }

        @Override
        public PCollectionTuple expand(PCollection<PubsubMessage> input) {
            PCollectionTuple destinationTableUdfOut =
                input
                    .apply("MapToRecord",
                        MapElements.into(CODER.getEncodedTypeDescriptor())
                            .via((PubsubMessage message) -> FailsafeElement.of(message, new String(message.getPayload(), StandardCharsets.UTF_8))))
                    .apply("DestinationTableUDF",
                        JavascriptUdfExecutor.FailsafeTableDestinationJavascriptUdf.<PubsubMessage>newBuilder()
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
                        FailsafeTransformJavascriptUdf.<PubsubMessage, TableDestination>newBuilder()
                            .setFileSystemPath(options.getJavascriptTextTableDestinationGcsPath())
                            .setFunctionName(options.getJavascriptTextTransformFunctionName())
                            .setSuccessTag(TRANSFORM_UDF_OUT)
                            .setFailureTag(TRANSFORM_UDF_DEADLETTER_OUT)
                            .build());

            PCollectionTuple jsonToTableRowOut =
                destinationTableUdfOut
                    .get(DESTINATION_TABLE_UDF_OUT)
                    .apply("jsonToTableRow",
                        BigQueryConverters.JsonToTableRow.<PubsubMessage>newBuilder()
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

package org.wakaba260.dataflow.templates;

import com.google.common.collect.ImmutableList;
import com.google.cloud.teleport.coders.FailsafeElementCoder;
import com.google.cloud.teleport.templates.common.ErrorConverters;
import com.google.cloud.teleport.util.ResourceUtils;
import com.google.cloud.teleport.values.FailsafeElement;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.Method;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.wakaba260.dataflow.templates.common.*;

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import static org.wakaba260.dataflow.templates.common.BigQueryInsertWithDeadLetter.*;
import static org.wakaba260.dataflow.templates.common.JavascriptUdfExecutor.*;

/**
 * The {@link PubSubToBigQueryDynamicDestination} pipeline is a streaming pipeline which ingests data in JSON format
 * from Cloud Pub/Sub, executes a transform and table destination UDF, and outputs the resulting records to BigQuery. Any errors
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
 * UDF_PATH=gs://${BUCKET_NAME}/dataflow/pipelines/pubsub-to-bigquery-dynamic-destination.js
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
 * --runner=${RUNNER} \
 * --useSubscription=${USE_SUBSCRIPTION}"
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
 * outputDeadletterTable=${PROJECT_ID}:dataset-id.deadletter-table,\
 * javascriptUdfGcsPath=${UDF_PATH},\
 * javascriptUdfGetTableNameFunctionName=getDestinationTable,\
 * javascriptUdfTransformFunctionName=transform,\
 * dynamicDestinationProject=${PROJECT_ID},\
 * dynamicDestinationDatasetId=${DATASET_ID}"
 *
 * # Execute a pipeline to read from a Subscription.
 * gcloud dataflow jobs run ${JOB_NAME} \
 * --gcs-location=${PIPELINE_FOLDER}/template \
 * --zone=us-east1-d \
 * --parameters \
 * "inputTopic=projects/${PROJECT_ID}/subscriptions/input-subscription-name,\
 * outputDeadletterTable=${PROJECT_ID}:dataset-id.deadletter-table,\
 * javascriptUdfGcsPath=${UDF_PATH},\
 * javascriptUdfGetTableNameFunctionName=getDestinationTable,\
 * javascriptUdfTransformFunctionName=transform,\
 * dynamicDestinationProject=${PROJECT_ID},\
 * dynamicDestinationDatasetId=${DATASET_ID}"
 * </pre>
 */
public class PubSubToBigQueryDynamicDestination {
    public interface Options extends DataflowPipelineOptions, JavascriptUdfExecutorOptions, BigQueryInsertWithDeadLetterOptions {
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

        @Description(
            "This determines whether the template reads from " + "a pub/sub subscription or a topic")
        @Default.Boolean(false)
        Boolean getUseSubscription();
        void setUseSubscription(Boolean value);
    }

    final static Coder<PubsubMessagePayload> PAYLOAD_CODER = PubsubMessagePayloadCoder.of();
    final static RecordCoder<PubsubMessagePayload> RECORD_CODER = RecordCoder.of(PAYLOAD_CODER);

    final static FailsafeElementCoder<String, String> FAILSAFE_ELEMENT_CODER =
        FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());

    final static TupleTag<Record<PubsubMessagePayload>> TRANSFORM_UDF_OUT = new TupleTag<Record<PubsubMessagePayload>>() {
    };

    final static TupleTag<Record<PubsubMessagePayload>> DESTINATION_TABLE_UDF_OUT = new TupleTag<Record<PubsubMessagePayload>>() {
    };

    static final TupleTag<Record<PubsubMessagePayload>> JSON_TO_TABLE_ROW_TRANSFORM_OUT = new TupleTag<Record<PubsubMessagePayload>>() {
    };

    final static TupleTag<Record<PubsubMessagePayload>> TRANSFORM_UDF_DEADLETTER_OUT = new TupleTag<Record<PubsubMessagePayload>>() {
    };

    final static TupleTag<Record<PubsubMessagePayload>> DESTINATION_TABLE_DEADLETTER_OUT = new TupleTag<Record<PubsubMessagePayload>>() {
    };

    static final TupleTag<Record<PubsubMessagePayload>> JSON_TO_TABLE_ROW_TRANSFORM_DEADLETTER_OUT = new TupleTag<Record<PubsubMessagePayload>>() {
    };

    public static void main(String[] args) {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

        run(options);
    }

    public static PipelineResult run(Options options) {
        Pipeline pipeline = Pipeline.create(options);

        PCollection<PubsubMessage> messages = null;

        CoderRegistry coderRegistry = pipeline.getCoderRegistry();
        coderRegistry.registerCoderForType(PAYLOAD_CODER.getEncodedTypeDescriptor(), PAYLOAD_CODER);
        coderRegistry.registerCoderForType(RECORD_CODER.getEncodedTypeDescriptor(), RECORD_CODER);

        FailsafeElementCoder bigQueryInsertCoder = BigQueryInsertWithDeadLetter.getCoder();
        coderRegistry.registerCoderForType(bigQueryInsertCoder.getEncodedTypeDescriptor(), bigQueryInsertCoder);

        options.setBigQueryWriteDisposition(WriteDisposition.WRITE_APPEND);
        options.setBigQueryCreateDisposition(CreateDisposition.CREATE_NEVER);
        options.setBigQueryInsertMethod(Method.STREAMING_INSERTS);

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
                .apply("MapToPayload",
                    MapElements.into(PAYLOAD_CODER.getEncodedTypeDescriptor())
                        .via((PubsubMessage message) -> PubsubMessagePayload.of(message)))
                .apply("ConvertMessageToTableRow", new PubSubMessageToTableRow(options));

        /*
         * Step #3: Write the successful records to BigQuery.
         */
        convertedTableRows
            .get(JSON_TO_TABLE_ROW_TRANSFORM_OUT)
            .apply(new BigQueryInsertWithDeadLetter(options));

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
            .apply("WrapErrors",
                MapElements.into(FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor())
                    .via((Record record) -> FailsafeElement.of(record.getOriginalPayload().getPayload(), record.getPayload())))
            .apply("WriteFailedRecords",
                ErrorConverters.WriteStringMessageErrors.newBuilder()
                    .setErrorRecordsTable(options.getOutputDeadletterTable())
                    .setErrorRecordsTableSchema(ResourceUtils.getDeadletterTableSchemaJson())
                    .build());

        return pipeline.run();
    }

    static class PubSubMessageToTableRow extends PTransform<PCollection<PubsubMessagePayload>, PCollectionTuple> {
        private final Options options;

        PubSubMessageToTableRow(Options options) {
            this.options = options;
        }

        @Override
        public PCollectionTuple expand(PCollection<PubsubMessagePayload> input) {
            PCollectionTuple destinationTableUdfOut =
                input
                    .apply("MapToRecord",
                        MapElements.into(RECORD_CODER.getEncodedTypeDescriptor())
                            .via(payload -> Record.of(payload)))
                    .apply("GetDestinationTableUDF",
                        RecordJavascriptGetTableDestinationUdf.<PubsubMessagePayload>newBuilder()
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
                        RecordJavascriptTransformUdf.<PubsubMessagePayload>newBuilder()
                            .setFileSystemPath(options.getJavascriptUdfGcsPath())
                            .setFunctionName(options.getJavascriptUdfTransformFunctionName())
                            .setSuccessTag(TRANSFORM_UDF_OUT)
                            .setFailureTag(TRANSFORM_UDF_DEADLETTER_OUT)
                            .build());

            PCollectionTuple jsonToTableRowOut =
                destinationTableUdfOut
                    .get(DESTINATION_TABLE_UDF_OUT)
                    .apply("jsonToTableRow",
                        BigQueryConverters.RecordToTableRow.<PubsubMessagePayload>newBuilder()
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

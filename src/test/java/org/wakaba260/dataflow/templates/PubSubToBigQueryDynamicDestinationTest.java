package org.wakaba260.dataflow.templates;

import avro.shaded.com.google.common.collect.ImmutableMap;
import com.google.api.services.bigquery.model.TableRow;
import com.google.common.io.Resources;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.wakaba260.dataflow.templates.common.PubsubMessagePayload;
import org.wakaba260.dataflow.templates.common.Record;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.wakaba260.dataflow.templates.PubSubToBigQueryDynamicDestination.*;

public class PubSubToBigQueryDynamicDestinationTest {
    @Rule public final transient TestPipeline pipeline = TestPipeline.create();

    private static final String RESOURCES_DIR = "JavascriptTextTransformerTest/";

    private static final String TRANSFORM_FILE_PATH =
        Resources.getResource(RESOURCES_DIR + "transform.js").getPath();

    @Test
    public void testPubSubToBigQueryE2E() {
        final String payload = "{\"ticker\": \"GOOGL\", \"price\": 1006.94, \"type\": \"payment\"}";

        final PubsubMessage message =
            new PubsubMessage(payload.getBytes(), ImmutableMap.of("id", "123", "type", "custom_event"));

        final Instant timestamp =
            new DateTime(2022, 2, 22, 22, 22, 22, 222, DateTimeZone.UTC).toInstant();

        CoderRegistry coderRegistry = pipeline.getCoderRegistry();
        coderRegistry.registerCoderForType(PAYLOAD_CODER.getEncodedTypeDescriptor(), PAYLOAD_CODER);
        coderRegistry.registerCoderForType(RECORD_CODER.getEncodedTypeDescriptor(), RECORD_CODER);

        // Parameters
        ValueProvider<String> transformPath = pipeline.newProvider(TRANSFORM_FILE_PATH);
        ValueProvider<String> transformFunction = pipeline.newProvider("transform");
        ValueProvider<String> destinationTableFunction = pipeline.newProvider("getDestinationTableName");
        ValueProvider<String> project = pipeline.newProvider("myProject");
        ValueProvider<String> datasetId = pipeline.newProvider("myDataset");

        Options options = PipelineOptionsFactory.create().as(Options.class);

        options.setJavascriptUdfGcsPath(transformPath);
        options.setJavascriptUdfTransformFunctionName(transformFunction);
        options.setJavascriptUdfGetTableNameFunctionName(destinationTableFunction);
        options.setDynamicDestinationProject(project);
        options.setDynamicDestinationDatasetId(datasetId);

        // Build pipeline
        PCollectionTuple transformOut =
            pipeline
                .apply(
                    "CreateInput",
                    Create.timestamped(TimestampedValue.of(message, timestamp))
                        .withCoder(PubsubMessageWithAttributesCoder.of()))
                .apply("MapToPayload",
                    MapElements.into(PAYLOAD_CODER.getEncodedTypeDescriptor())
                        .via((PubsubMessage m) -> PubsubMessagePayload.of(m)))
                .apply("ConvertMessageToTableRow", new PubSubMessageToTableRow(options));

        // Assert
        PAssert.that(transformOut.get(TRANSFORM_UDF_DEADLETTER_OUT)).empty();
        PAssert.that(transformOut.get(JSON_TO_TABLE_ROW_TRANSFORM_DEADLETTER_OUT)).empty();
        PAssert.that(transformOut.get(JSON_TO_TABLE_ROW_TRANSFORM_OUT))
            .satisfies(
                collection -> {
                    Record result = collection.iterator().next();
                    TableDestination destination = result.getDestination();
                    TableRow row = result.getRow();
                    assertThat(row.get("ticker"), is(equalTo("GOOGL")));
                    assertThat(row.get("price"), is(equalTo(1006.94)));
                    assertThat(destination.getTableSpec(), is(equalTo("myProject:myDataset.payment")));
                    return null;
                });

        // Execute pipeline
        pipeline.run();
    }
}

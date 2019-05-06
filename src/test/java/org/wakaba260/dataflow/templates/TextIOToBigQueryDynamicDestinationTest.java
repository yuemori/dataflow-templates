package org.wakaba260.dataflow.templates;

import com.google.api.services.bigquery.model.TableRow;
import com.google.common.io.Resources;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.*;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.wakaba260.dataflow.templates.TextIOToBigQueryDynamicDestination.*;

public class TextIOToBigQueryDynamicDestinationTest {
    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    private static final String RESOURCES_DIR = "JavascriptTextTransformerTest/";

    private static final String TRANSFORM_FILE_PATH =
        Resources.getResource(RESOURCES_DIR + "transform.js").getPath();

    @Test
    public void testTextIOToBigQueryE2E() {
        final String payload = "{\"ticker\": \"GOOGL\", \"price\": 1006.94, \"type\": \"payment\"}";

        final Instant timestamp =
            new DateTime(2022, 2, 22, 22, 22, 22, 222, DateTimeZone.UTC).toInstant();

        CoderRegistry coderRegistry = pipeline.getCoderRegistry();
        coderRegistry.registerCoderForType(PAYLOAD_CODER.getEncodedTypeDescriptor(), PAYLOAD_CODER);
        coderRegistry.registerCoderForType(FAILSAFE_PAYLOAD_CODER.getEncodedTypeDescriptor(), FAILSAFE_PAYLOAD_CODER);
        coderRegistry.registerCoderForType(FAILSAFE_TABLE_ROW_CODER.getEncodedTypeDescriptor(), FAILSAFE_TABLE_ROW_CODER);

        ValueProvider<String> transformPath = pipeline.newProvider(TRANSFORM_FILE_PATH);
        ValueProvider<String> transformFunction = pipeline.newProvider("transform");
        ValueProvider<String> destinationTableFunction = pipeline.newProvider("resolveDestination");
        ValueProvider<String> project = pipeline.newProvider("myProject");
        ValueProvider<String> datasetId = pipeline.newProvider("myDataset");

        Options options = PipelineOptionsFactory.create().as(Options.class);

        options.setJavascriptTextTableDestinationGcsPath(transformPath);
        options.setJavascriptTextTransformFunctionName(transformFunction);
        options.setJavascriptTextTableDestinationFunctionName(destinationTableFunction);
        options.setDestinationProject(project);
        options.setDestinationDatasetId(datasetId);

        PCollectionTuple transformOut =
           pipeline
               .apply("CreateInput", Create.timestamped(TimestampedValue.of(payload, timestamp)))
               .apply("ConvertMessageToTableRow", new StringToTableRow(options));

        PAssert.that(transformOut.get(TRANSFORM_UDF_DEADLETTER_OUT)).empty();
        PAssert.that(transformOut.get(JSON_TO_TABLE_ROW_TRANSFORM_DEADLETTER_OUT)).empty();
        PAssert.that(transformOut.get(JSON_TO_TABLE_ROW_TRANSFORM_OUT))
            .satisfies(
                collection -> {
                    KV<TableDestination, TableRow> result = collection.iterator().next();
                    TableDestination destination = result.getKey();
                    TableRow row = result.getValue();
                    assertThat(row.get("ticker"), is(equalTo("GOOGL")));
                    assertThat(row.get("price"), is(equalTo(1006.94)));
                    assertThat(destination.getTableSpec(), is(equalTo("myProject:myDataset.payment")));
                    return null;
                });

        pipeline.run();
    }
}

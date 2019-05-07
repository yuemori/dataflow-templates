package org.wakaba260.dataflow.templates;

import com.google.api.services.bigquery.model.TableRow;
import com.google.common.io.Resources;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.*;
import org.junit.Rule;
import org.junit.Test;
import org.wakaba260.dataflow.templates.common.Record;
import org.wakaba260.dataflow.templates.common.StringPayload;

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

        CoderRegistry coderRegistry = pipeline.getCoderRegistry();
        coderRegistry.registerCoderForType(PAYLOAD_CODER.getEncodedTypeDescriptor(), PAYLOAD_CODER);
        coderRegistry.registerCoderForType(RECORD_CODER.getEncodedTypeDescriptor(), RECORD_CODER);

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

        PCollectionTuple transformOut =
            pipeline
               .apply("ReadFromSource", Create.of(Arrays.asList(payload)))
               .apply("MapToPayload",
                   MapElements.into(PAYLOAD_CODER.getEncodedTypeDescriptor())
                       .via((String message) -> StringPayload.of(message)))
               .apply("ConvertMessageToTableRow", new StringToTableRow(options));

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

        pipeline.run();
    }
}

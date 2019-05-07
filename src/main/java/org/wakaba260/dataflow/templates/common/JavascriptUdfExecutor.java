package org.wakaba260.dataflow.templates.common;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.templates.common.JavascriptTextTransformer;
import com.google.common.base.Strings;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

import javax.annotation.Nullable;

@AutoValue
public abstract class JavascriptUdfExecutor {
    private static JavascriptTextTransformer.JavascriptRuntime getJavascriptRuntime(String fileSystemPath, String functionName) {
        JavascriptTextTransformer.JavascriptRuntime javascriptRuntime = null;
        if (!Strings.isNullOrEmpty(fileSystemPath) && !Strings.isNullOrEmpty(functionName)) {
            javascriptRuntime = JavascriptTextTransformer.JavascriptRuntime.newBuilder().setFunctionName(functionName).setFileSystemPath(fileSystemPath).build();
        }

        return javascriptRuntime;
    }

    @AutoValue
    public abstract static class RecordJavascriptGetTableDestinationUdf<T extends Payload> extends PTransform<PCollection<Record<T>>, PCollectionTuple> {
        RecordJavascriptGetTableDestinationUdf() {
        }

        public static <T extends Payload> Builder<T> newBuilder() {
            return new AutoValue_JavascriptUdfExecutor_RecordJavascriptGetTableDestinationUdf.Builder<T>();
        }

        public abstract ValueProvider<String> fileSystemPath();
        public abstract ValueProvider<String> functionName();
        public abstract ValueProvider<String> projectName();
        public abstract ValueProvider<String> datasetId();

        public abstract TupleTag<Record<T>> successTag();

        public abstract TupleTag<Record<T>> failureTag();

        public class Processor<T extends Payload> extends DoFn<Record<T>, Record<T>> {
            private final TupleTag<Record<T>> failureTag;
            private JavascriptTextTransformer.JavascriptRuntime javascriptRuntime;
            private String projectName;
            private String datasetId;
            
            public Processor(TupleTag<Record<T>> failureTag) {
                this.failureTag = failureTag;
            }

            @Setup
            public void setup() {
                if (RecordJavascriptGetTableDestinationUdf.this.fileSystemPath() != null && RecordJavascriptGetTableDestinationUdf.this.functionName() != null) {
                    this.javascriptRuntime = getJavascriptRuntime(RecordJavascriptGetTableDestinationUdf.this.fileSystemPath().get(), RecordJavascriptGetTableDestinationUdf.this.functionName().get());
                }
                this.projectName = RecordJavascriptGetTableDestinationUdf.this.projectName().get();
                this.datasetId = RecordJavascriptGetTableDestinationUdf.this.datasetId().get();
            }

            @ProcessElement
            public void processElement(ProcessContext context) {
                Record<T> element = context.element();
                String payloadStr = element.getPayload();
                TableDestination destination = null;
                String udfOut = "";

                try {
                    if (this.javascriptRuntime != null) {
                        udfOut = this.javascriptRuntime.invoke(payloadStr);

                        if(!Strings.isNullOrEmpty(udfOut)) {
                            String destinationName = String.format(
                                "%s:%s.%s",
                                this.projectName,
                                this.datasetId,
                                udfOut);
                            destination = new TableDestination(destinationName, destinationName);
                        }
                    }

                    if (destination != null) {
                        context.output(Record.<T>of(element, destination));
                    } else {
                        throw new RuntimeException(String.format("An handled destination table: %s:%s.%s", this.projectName, this.datasetId, udfOut));
                    }
                } catch (Exception var5) {
                    context.output(this.failureTag, Record.of(element, var5));
                }
            }
        }

        public PCollectionTuple expand(PCollection<Record<T>> elements) {
            return elements.apply("ProcessGetTableDestinationUdf",
                ParDo.of(new Processor<T>(this.failureTag()))
                    .withOutputTags(this.successTag(), TupleTagList.of(this.failureTag())));
        }

        @AutoValue.Builder
        public abstract static class Builder<T extends Payload> {
            public Builder() {
            }

            public abstract Builder<T> setFileSystemPath(ValueProvider<String> var1);

            public abstract Builder<T> setFunctionName(ValueProvider<String> var1);

            public abstract Builder<T> setProjectName(ValueProvider<String> var1);

            public abstract Builder<T> setDatasetId(ValueProvider<String> var1);

            public abstract Builder<T> setSuccessTag(TupleTag<Record<T>> var1);

            public abstract Builder<T> setFailureTag(TupleTag<Record<T>> var1);

            public abstract RecordJavascriptGetTableDestinationUdf<T> build();
        }
    }

    @AutoValue
    public abstract static class RecordJavascriptTransformUdf<T extends Payload> extends PTransform<PCollection<Record<T>>, PCollectionTuple> {
        RecordJavascriptTransformUdf() {
        }

        public static <T extends Payload> Builder<T> newBuilder() {
            return new AutoValue_JavascriptUdfExecutor_RecordJavascriptTransformUdf.Builder<T>();
        }

        public abstract ValueProvider<String> fileSystemPath();
        @Nullable
        public abstract ValueProvider<String> functionName();

        public abstract TupleTag<Record<T>> successTag();
        public abstract TupleTag<Record<T>> failureTag();

        class Processor<T extends Payload> extends DoFn<Record<T>, Record<T>> {
            private final TupleTag<Record<T>> failureTag;
            private JavascriptTextTransformer.JavascriptRuntime javascriptRuntime;

            public Processor(TupleTag<Record<T>> failureTag) {
                this.failureTag = failureTag;
            }

            @Setup
            public void setup() {
                if (RecordJavascriptTransformUdf.this.fileSystemPath() != null && RecordJavascriptTransformUdf.this.functionName() != null) {
                    this.javascriptRuntime = getJavascriptRuntime(RecordJavascriptTransformUdf.this.fileSystemPath().get(), RecordJavascriptTransformUdf.this.functionName().get());
                }
            }

            @ProcessElement
            public void processElement(ProcessContext context) {
                Record<T> element = context.element();
                String payloadStr = element.getOriginalPayloadString();

                try {
                    if (this.javascriptRuntime != null) {
                        payloadStr = this.javascriptRuntime.invoke(payloadStr);
                    }

                    if (!Strings.isNullOrEmpty(payloadStr)) {
                        context.output(Record.<T>of(element, payloadStr));
                    }
                } catch (Exception var5) {
                    context.output(this.failureTag, Record.of(element, var5));
                }
            }
        }

        public PCollectionTuple expand(PCollection<Record<T>> elements) {
            return elements.apply("ProcessTransformUdf",
                    ParDo.of(new Processor<T>(this.failureTag()))
                        .withOutputTags(this.successTag(), TupleTagList.of(this.failureTag())));
        }

        @AutoValue.Builder
        public abstract static class Builder<T extends Payload> {
            public Builder() {
            }

            public abstract Builder<T> setFileSystemPath(@Nullable ValueProvider<String> var1);

            public abstract Builder<T> setFunctionName(@Nullable ValueProvider<String> var1);

            public abstract Builder<T> setSuccessTag(TupleTag<Record<T>> var1);

            public abstract Builder<T> setFailureTag(TupleTag<Record<T>> var1);

            public abstract RecordJavascriptTransformUdf<T> build();
        }
    }

    public interface JavascriptUdfExecutorOptions extends PipelineOptions {
        @Validation.Required
        @Description("Gcs path to javascript udf source")
        ValueProvider<String> getJavascriptUdfGcsPath();

        void setJavascriptUdfGcsPath(ValueProvider<String> var1);

        @Description("UDF Javascript Transform Function Name")
        ValueProvider<String> getJavascriptUdfTransformFunctionName();

        void setJavascriptUdfTransformFunctionName(ValueProvider<String> var1);

        @Validation.Required
        @Description("UDF Javascript Get Destination Table Name Function Name")
        ValueProvider<String> getJavascriptUdfGetTableNameFunctionName();

        void setJavascriptUdfGetTableNameFunctionName(ValueProvider<String> var1);

        @Validation.Required
        @Description("Destination Project Name")
        ValueProvider<String> getDynamicDestinationProject();
        void setDynamicDestinationProject(ValueProvider<String> project);

        @Validation.Required
        @Description("Destination Dataset Id")
        ValueProvider<String> getDynamicDestinationDatasetId();
        void setDynamicDestinationDatasetId(ValueProvider<String> datasetId);
    }
}

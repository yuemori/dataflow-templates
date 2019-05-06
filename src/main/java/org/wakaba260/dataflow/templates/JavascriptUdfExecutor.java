package org.wakaba260.dataflow.templates;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.templates.common.JavascriptTextTransformer;
import com.google.cloud.teleport.values.FailsafeElement;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
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
    public abstract static class FailsafeTransformJavascriptUdf<T, T2> extends PTransform<PCollection<FailsafeElement<T, KV<T2, String>>>, PCollectionTuple> {
        FailsafeTransformJavascriptUdf() {
        }

        public abstract ValueProvider<String> fileSystemPath();
        @Nullable
        public abstract ValueProvider<String> functionName();

        public abstract TupleTag<FailsafeElement<T, KV<T2, String>>> successTag();
        public abstract TupleTag<FailsafeElement<T, String>> failureTag();

        public static <T, T2> Builder<T, T2> newBuilder() {
            return new AutoValue_JavascriptUdfExecutor_FailsafeTransformJavascriptUdf.Builder<T, T2>();
        }

        public PCollectionTuple expand(PCollection<FailsafeElement<T, KV<T2, String>>> elements) {
          return (PCollectionTuple)elements.apply("ProcessTransformUdf", ParDo.of(new DoFn<FailsafeElement<T, KV<T2, String>>, FailsafeElement<T, KV<T2, String>>>() {
              private JavascriptTextTransformer.JavascriptRuntime javascriptRuntime;

              @Setup
              public void setup() {
                  if (FailsafeTransformJavascriptUdf.this.fileSystemPath() != null && FailsafeTransformJavascriptUdf.this.functionName() != null) {
                      this.javascriptRuntime = getJavascriptRuntime(FailsafeTransformJavascriptUdf.this.fileSystemPath().get(), FailsafeTransformJavascriptUdf.this.functionName().get());
                  }
              }

              @ProcessElement
              public void processElement(ProcessContext context) {
                  FailsafeElement<T, KV<T2, String>> element = context.element();
                  String payloadStr = element.getPayload().getValue();

                  try {
                      if (this.javascriptRuntime != null) {
                          payloadStr = this.javascriptRuntime.invoke(payloadStr);
                      }

                      if (!Strings.isNullOrEmpty(payloadStr)) {
                          context.output(FailsafeElement.of(element.getOriginalPayload(), KV.of(element.getPayload().getKey(), payloadStr)));
                      }
                  } catch (Exception var5) {
                      context.output(FailsafeTransformJavascriptUdf.this.failureTag(), FailsafeElement.of(element.getOriginalPayload(), element.getPayload().getValue()).setErrorMessage(var5.getMessage()).setStacktrace(Throwables.getStackTraceAsString(var5)));
                  }
              }

          }).withOutputTags(this.successTag(), TupleTagList.of(this.failureTag())));
        }

        @AutoValue.Builder
        public abstract static class Builder<T, T2> {
            public Builder() {
            }

            public abstract Builder<T, T2> setFileSystemPath(@Nullable ValueProvider<String> var1);

            public abstract Builder<T, T2> setFunctionName(@Nullable ValueProvider<String> var1);

            public abstract Builder<T, T2> setSuccessTag(TupleTag<FailsafeElement<T, KV<T2, String>>> var1);

            public abstract Builder<T, T2> setFailureTag(TupleTag<FailsafeElement<T, String>> var1);

            public abstract FailsafeTransformJavascriptUdf<T, T2> build();
        }

        public interface TransformUdfOptions extends PipelineOptions {
            @Description("Gcs path to javascript udf source")
            ValueProvider<String> getJavascriptTextTransformGcsPath();

            void setJavascriptTextTransformGcsPath(ValueProvider<String> var1);

            @Description("UDF Javascript Transform Function Name")
            ValueProvider<String> getJavascriptTextTransformFunctionName();

            void setJavascriptTextTransformFunctionName(ValueProvider<String> var1);
        }
    }

    @AutoValue
    public abstract static class FailsafeTableDestinationJavascriptUdf<T> extends PTransform<PCollection<FailsafeElement<T, String>>, PCollectionTuple> {
        FailsafeTableDestinationJavascriptUdf() {
        }

        public abstract ValueProvider<String> fileSystemPath();
        public abstract ValueProvider<String> functionName();
        public abstract ValueProvider<String> projectName();
        public abstract ValueProvider<String> datasetId();

        public abstract TupleTag<FailsafeElement<T, KV<TableDestination, String>>> successTag();

        public abstract TupleTag<FailsafeElement<T, String>> failureTag();

        public static <T> Builder<T> newBuilder() {
            return new AutoValue_JavascriptUdfExecutor_FailsafeTableDestinationJavascriptUdf.Builder<T>();
        }

        public PCollectionTuple expand(PCollection<FailsafeElement<T, String>> elements) {
            return (PCollectionTuple)elements.apply("ProcessUdf", ParDo.of(new DoFn<FailsafeElement<T, String>, FailsafeElement<T, KV<TableDestination, String>>>() {
                private JavascriptTextTransformer.JavascriptRuntime javascriptRuntime;
                private String projectName;
                private String datasetId;

                @Setup
                public void setup() {
                    if (FailsafeTableDestinationJavascriptUdf.this.fileSystemPath() != null && FailsafeTableDestinationJavascriptUdf.this.functionName() != null) {
                        this.javascriptRuntime = getJavascriptRuntime(FailsafeTableDestinationJavascriptUdf.this.fileSystemPath().get(), FailsafeTableDestinationJavascriptUdf.this.functionName().get());
                    }
                    this.projectName = FailsafeTableDestinationJavascriptUdf.this.projectName().get();
                    this.datasetId = FailsafeTableDestinationJavascriptUdf.this.datasetId().get();
                }

                @ProcessElement
                public void processElement(ProcessContext context) {
                    FailsafeElement<T, String> element = context.element();
                    String payloadStr = element.getPayload();
                    TableDestination destination = null;

                    try {
                        if (this.javascriptRuntime != null) {
                            String udfOut = this.javascriptRuntime.invoke(payloadStr);

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
                            context.output(FailsafeElement.of(element.getOriginalPayload(), KV.of(destination, payloadStr)));
                        } else {
                            throw new RuntimeException("An handled destination table");
                        }
                    } catch (Exception var5) {
                        context.output(FailsafeTableDestinationJavascriptUdf.this.failureTag(), FailsafeElement.of(element).setErrorMessage(var5.getMessage()).setStacktrace(Throwables.getStackTraceAsString(var5)));
                    }
                }
            }).withOutputTags(this.successTag(), TupleTagList.of(this.failureTag())));
        }

        @AutoValue.Builder
        public abstract static class Builder<T> {
            public Builder() {
            }

            public abstract Builder<T> setFileSystemPath(ValueProvider<String> var1);

            public abstract Builder<T> setFunctionName(ValueProvider<String> var1);

            public abstract Builder<T> setProjectName(ValueProvider<String> var1);

            public abstract Builder<T> setDatasetId(ValueProvider<String> var1);

            public abstract Builder<T> setSuccessTag(TupleTag<FailsafeElement<T, KV<TableDestination, String>>> var1);

            public abstract Builder<T> setFailureTag(TupleTag<FailsafeElement<T, String>> var1);

            public abstract FailsafeTableDestinationJavascriptUdf<T> build();
        }

        public interface TableDestinationUdfOptions extends PipelineOptions {
            @Validation.Required
            @Description("Gcs path to javascript udf source")
            ValueProvider<String> getJavascriptTextTableDestinationGcsPath();

            void setJavascriptTextTableDestinationGcsPath(ValueProvider<String> var1);

            @Validation.Required
            @Description("UDF Javascript Destination Table Function Name")
            ValueProvider<String> getJavascriptTextTableDestinationFunctionName();

            void setJavascriptTextTableDestinationFunctionName(ValueProvider<String> var1);

            @Validation.Required
            @Description("Destination Project Name")
            ValueProvider<String> getDestinationProject();
            void setDestinationProject(ValueProvider<String> project);

            @Validation.Required
            @Description("Destination Dataset Id")
            ValueProvider<String> getDestinationDatasetId();
            void setDestinationDatasetId(ValueProvider<String> datasetId);
        }
    }
}

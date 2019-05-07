package org.wakaba260.dataflow.templates.common;

import com.google.api.services.bigquery.model.TableRow;
import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.values.FailsafeElement;
import com.google.common.base.Throwables;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

@AutoValue
public abstract class BigQueryConverters {
    @AutoValue
    public abstract static class RecordToTableRow<T extends Payload> extends PTransform<PCollection<Record<T>>, PCollectionTuple> {
        RecordToTableRow() {
        }

        public static <T extends Payload> RecordToTableRow.Builder<T> newBuilder() {
            return new AutoValue_BigQueryConverters_RecordToTableRow.Builder<T>();
        }

        public abstract TupleTag<Record<T>> successTag();

        public abstract TupleTag<Record<T>> failureTag();

        public class Processor<T extends Payload> extends DoFn<Record<T>, Record<T>> {
            private final TupleTag<Record<T>> failureTag;

            public Processor(TupleTag<Record<T>> failureTag) {
                this.failureTag = failureTag;
            }

            @ProcessElement
            public void processElement(ProcessContext context) {
                Record<T> record = context.element();
                String payload = record.getPayload();

                try {
                    TableRow row = convertJsonToTableRow(payload);
                    context.output(Record.<T>of(record, row));
                } catch (Exception e) {
                    context.output(this.failureTag, Record.of(record, e));
                }
            }
        }

        public PCollectionTuple expand(PCollection<Record<T>> elements) {
            return elements.apply("RecordToTableRow",
                ParDo.of(new Processor<T>(this.failureTag()))
                    .withOutputTags(this.successTag(), TupleTagList.of(this.failureTag())));
        }


        private static TableRow convertJsonToTableRow(String json) {
            TableRow row;
            // Parse the JSON into a {@link TableRow} object.
            try (InputStream inputStream = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))) {
                row = TableRowJsonCoder.of().decode(inputStream, Coder.Context.OUTER);
            } catch (IOException e) {
                throw new RuntimeException("Failed to serialize json to table row: " + json, e);
            }

            return row;
        }

        @AutoValue.Builder
        public abstract static class Builder<T extends Payload> {
            public Builder() {
            }

            public abstract RecordToTableRow.Builder<T> setSuccessTag(TupleTag<Record<T>> var1);

            public abstract RecordToTableRow.Builder<T> setFailureTag(TupleTag<Record<T>> var1);

            public abstract RecordToTableRow<T> build();
        }

    }


    @AutoValue
    public abstract static class JsonToTableRow<T> extends PTransform<PCollection<FailsafeElement<T, KV<TableDestination, String>>>, PCollectionTuple> {
        JsonToTableRow() {
        }

        public abstract TupleTag<KV<TableDestination, TableRow>> successTag();

        public abstract TupleTag<FailsafeElement<T, String>> failureTag();

        public static <T> JsonToTableRow.Builder<T> newBuilder() {
            return new AutoValue_BigQueryConverters_JsonToTableRow.Builder();
        }

        public PCollectionTuple expand(PCollection<FailsafeElement<T, KV<TableDestination, String>>> failsafeElements) {
            return (PCollectionTuple)failsafeElements.apply("JsonToTableRow", ParDo.of(new DoFn<FailsafeElement<T, KV<TableDestination, String>>, KV<TableDestination, TableRow>>() {
                @DoFn.ProcessElement
                public void processElement(DoFn<FailsafeElement<T, KV<TableDestination, String>>, KV<TableDestination, TableRow>>.ProcessContext context) {
                FailsafeElement<T, KV<TableDestination, String>> element = context.element();
                TableDestination destination = element.getPayload().getKey();
                String payload = element.getPayload().getValue();

                try {
                    TableRow row = convertJsonToTableRow(payload);
                    context.output(KV.of(destination, row));
                } catch (Exception var5) {
                    context.output(JsonToTableRow.this.failureTag(), FailsafeElement.of(element.getOriginalPayload(), element.getPayload().getValue()).setErrorMessage(var5.getMessage()).setStacktrace(Throwables.getStackTraceAsString(var5)));
                }
                }
            }).withOutputTags(this.successTag(), TupleTagList.of(this.failureTag())));
        }

        private static TableRow convertJsonToTableRow(String json) {
            TableRow row;
            // Parse the JSON into a {@link TableRow} object.
            try (InputStream inputStream = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))) {
                row = TableRowJsonCoder.of().decode(inputStream, Coder.Context.OUTER);
            } catch (IOException e) {
                throw new RuntimeException("Failed to serialize json to table row: " + json, e);
            }

            return row;
        }

        @com.google.auto.value.AutoValue.Builder
        public abstract static class Builder<T> {
            public Builder() {
            }

            public abstract JsonToTableRow.Builder<T> setSuccessTag(TupleTag<KV<TableDestination, TableRow>> var1);

            public abstract JsonToTableRow.Builder<T> setFailureTag(TupleTag<FailsafeElement<T, String>> var1);

            public abstract JsonToTableRow<T> build();
        }
    }

}

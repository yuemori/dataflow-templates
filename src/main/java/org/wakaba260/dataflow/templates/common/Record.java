package org.wakaba260.dataflow.templates.common;

import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.model.TableRow;
import com.google.common.base.Throwables;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;

public class Record<T extends Payload> {
    private T originalPayload;
    private TableDestination destination;
    private TableRow row;
    private String payload;
    private String errorMessage;
    private String stackTrace;

    private Record(T originalPayload) {
        this.originalPayload = originalPayload;
        this.payload = originalPayload.getPayload();
    }

    private Record(T originalPayload, String payload, TableDestination destination, TableRow row, String errorMessage, String stackTrace) {
        this.originalPayload = originalPayload;
        this.payload = payload;
        this.destination = destination;
        this.row = row;
        this.errorMessage = errorMessage;
        this.stackTrace = stackTrace;
    }

    public static <T extends Payload> Record<T> of(T originalPayload) {
        return new Record(originalPayload);
    }

    public static <T extends Payload> Record of(Record record, TableDestination destination) {
        return of(
            record.getOriginalPayload(),
            record.payload,
            destination,
            record.row,
            record.errorMessage,
            record.stackTrace);
    }

    public static <T extends Payload> Record of(Record record, TableRow row) {
        return of(
                record.getOriginalPayload(),
                record.payload,
                record.destination,
                row,
                record.errorMessage,
                record.stackTrace);
    }

    public static <T extends Payload> Record<T> of(Record<T> record, Exception e) {
        return of(
            record.getOriginalPayload(),
            record.payload,
            record.destination,
            record.row,
            e.getMessage(),
            Throwables.getStackTraceAsString(e));
    }

    public static <T extends Payload> Record<T> of(Record<T> record, String newPayload) {
        return of(
            record.getOriginalPayload(),
            newPayload,
            record.destination,
            record.row,
            record.errorMessage,
            record.stackTrace);
    }

    public static <T extends Payload> Record<T> of(T originalPayload, String payload, TableDestination destination, TableRow row, String errorMessage, String stackTrace) {
        return new Record(
            originalPayload,
            payload,
            destination,
            row,
            errorMessage,
            stackTrace);
    }

    public String getPayload() {
        return this.payload;
    }

    public T getOriginalPayload() {
        return originalPayload;
    }

    public String getOriginalPayloadString() {
        return originalPayload.getPayload();
    }

    public TableDestination getDestination() {
        return this.destination;
    }

    public TableRow getRow() {
        return this.row;
    }

    public String getErrorMessage() {
        return this.errorMessage;
    }

    public String getStackTrace() {
        return this.stackTrace;
    }
}

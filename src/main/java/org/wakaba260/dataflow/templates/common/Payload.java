package org.wakaba260.dataflow.templates.common;

public interface Payload<T> {
    String getPayload();
    T getOriginalPayload();
}

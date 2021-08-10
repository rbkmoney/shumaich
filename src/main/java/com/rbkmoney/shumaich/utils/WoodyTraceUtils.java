package com.rbkmoney.shumaich.utils;

import com.rbkmoney.woody.api.trace.Span;
import com.rbkmoney.woody.api.trace.TraceData;
import com.rbkmoney.woody.api.trace.context.TraceContext;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class WoodyTraceUtils {

    public static String getSpanId() {
        Span span = getActiveSpan();
        if (span == null) {
            return null;
        }
        return span.getId();
    }

    public static String getTraceId() {
        Span span = getActiveSpan();
        if (span == null) {
            return null;
        }
        return span.getTraceId();
    }

    public static String getParentId() {
        Span span = getActiveSpan();
        if (span == null) {
            return null;
        }
        return span.getParentId();
    }

    public static Span getActiveSpan() {
        final TraceData currentTraceData = TraceContext.getCurrentTraceData();
        if (currentTraceData == null) {
            return null;
        }
        if (currentTraceData.getActiveSpan() == null) {
            return null;
        }
        return currentTraceData.getActiveSpan().getSpan();
    }
}

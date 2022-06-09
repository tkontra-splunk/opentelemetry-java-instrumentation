/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.kafka.internal;

import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.context.Context;
import io.opentelemetry.instrumentation.api.instrumenter.AttributesExtractor;
import java.util.List;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;

/**
 * Extracts the specified "captured headers" (see KafkaMessageCapturedHeadersUtil) from a
 * REQUEST message.
 *
 * This class is internal and is hence not for public use. Its APIs are unstable and can change at
 * any time.
 */
public final class KafkaHeadersAttributesExtractor<REQUEST>
    implements AttributesExtractor<REQUEST, Void> {

  /**
   * List of header names to capture as attributes.
   */
  private final List<String> capturedMessageHeaders;
  /**
   * Lens to get the Headers collection from a kafka message.
   */
  private final Function<REQUEST, Headers> messageHeadersGetter;
  private final KafkaHeadersGetter kafkaHeadersGetter = new KafkaHeadersGetter();

  public static final Function<? super ConsumerRecord<?, ?>, Headers> CONSUMER_RECORD_HEADERS_GETTER =
      ConsumerRecord::headers;

  public static final Function<? super ProducerRecord<?, ?>, Headers> PRODUCER_RECORD_HEADERS_GETTER =
      ProducerRecord::headers;

  public KafkaHeadersAttributesExtractor(
      List<String> capturedMessageHeaders,
      Function<REQUEST, Headers> headersGetter) {
    this.capturedMessageHeaders = capturedMessageHeaders;
    this.messageHeadersGetter = headersGetter;
  }

  @Override
  public void onStart(AttributesBuilder attributes, Context parentContext, REQUEST message) {
    Headers headers = this.messageHeadersGetter.apply(message);
    for (String headerName: capturedMessageHeaders) {
      String value = this.kafkaHeadersGetter.get(headers, headerName);
      if (value != null) {
        attributes.put(headerName, value);
      }
    }
  }

  @Override
  public void onEnd(
      AttributesBuilder attributes,
      Context context,
      REQUEST message,
      @Nullable Void unused,
      @Nullable Throwable error) {}
}

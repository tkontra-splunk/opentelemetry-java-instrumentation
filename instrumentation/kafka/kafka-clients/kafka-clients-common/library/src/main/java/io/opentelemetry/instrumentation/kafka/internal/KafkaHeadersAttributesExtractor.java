/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.kafka.internal;

import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.context.Context;
import io.opentelemetry.instrumentation.api.instrumenter.AttributesExtractor;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;

/**
 * This class is internal and is hence not for public use. Its APIs are unstable and can change at
 * any time.
 */
public final class KafkaHeadersAttributesExtractor<REQUEST>
    implements AttributesExtractor<REQUEST, Void> {

  public abstract static class KafkaMessageHeadersGetter<REQUEST> {
    public abstract Headers get(REQUEST message);
  }

  public static final KafkaMessageHeadersGetter<? super ConsumerRecord<?, ?>> CONSUMER_RECORD_HEADERS_GETTER =
      new KafkaMessageHeadersGetter<ConsumerRecord<?, ?>>() {
        @Override
        public Headers get(ConsumerRecord<?, ?> message) {
          return message.headers();
        }
      };

  public static final KafkaMessageHeadersGetter<? super ProducerRecord<?, ?>> PRODUCER_RECORD_HEADERS_GETTER =
      new KafkaMessageHeadersGetter<ProducerRecord<?, ?>>() {
        @Override
        public Headers get(ProducerRecord<?, ?> message) {
          return message.headers();
        }
      };

  private final List<String> capturedMessageHeaders;
  private final KafkaMessageHeadersGetter<REQUEST> messageHeaderGetter;
  private final KafkaHeadersGetter kafkaHeadersGetter = new KafkaHeadersGetter();

  public KafkaHeadersAttributesExtractor(List<String> capturedMessageHeaders,
      KafkaMessageHeadersGetter<REQUEST> headersGetter) {
    this.capturedMessageHeaders = capturedMessageHeaders;
    this.messageHeaderGetter = headersGetter;
  }

  @Override
  public void onStart(AttributesBuilder attributes, Context parentContext, REQUEST message) {
    Headers headers = this.messageHeaderGetter.get(message);
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

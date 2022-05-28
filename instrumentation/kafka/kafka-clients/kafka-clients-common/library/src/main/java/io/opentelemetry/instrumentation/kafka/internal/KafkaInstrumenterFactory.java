/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.kafka.internal;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.instrumentation.api.config.ExperimentalConfig;
import io.opentelemetry.instrumentation.api.instrumenter.AttributesExtractor;
import io.opentelemetry.instrumentation.api.instrumenter.ErrorCauseExtractor;
import io.opentelemetry.instrumentation.api.instrumenter.Instrumenter;
import io.opentelemetry.instrumentation.api.instrumenter.InstrumenterBuilder;
import io.opentelemetry.instrumentation.api.instrumenter.SpanKindExtractor;
import io.opentelemetry.instrumentation.api.instrumenter.SpanLinksExtractor;
import io.opentelemetry.instrumentation.api.instrumenter.messaging.MessageOperation;
import io.opentelemetry.instrumentation.api.instrumenter.messaging.MessagingAttributesExtractor;
import io.opentelemetry.instrumentation.api.instrumenter.messaging.MessagingSpanNameExtractor;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * This class is internal and is hence not for public use. Its APIs are unstable and can change at
 * any time.
 */
public final class KafkaInstrumenterFactory {

  private final OpenTelemetry openTelemetry;
  private final String instrumentationName;
  private ErrorCauseExtractor errorCauseExtractor = ErrorCauseExtractor.jdk();
  private final List<String> capturedConsumerMessageHeaders =
      KafkaMessageCapturedHeadersUtil.consumerMessageHeaders;
  private final List<String> capturedProducerMessageHeaders =
      KafkaMessageCapturedHeadersUtil.producerMessageHeaders;

  public KafkaInstrumenterFactory(OpenTelemetry openTelemetry, String instrumentationName) {
    this.openTelemetry = openTelemetry;
    this.instrumentationName = instrumentationName;
  }

  public KafkaInstrumenterFactory setErrorCauseExtractor(ErrorCauseExtractor errorCauseExtractor) {
    this.errorCauseExtractor = errorCauseExtractor;
    return this;
  }

  public Instrumenter<ProducerRecord<?, ?>, Void> createProducerInstrumenter() {
    return createProducerInstrumenter(Collections.emptyList());
  }

  public Instrumenter<ProducerRecord<?, ?>, Void> createProducerInstrumenter(
      Iterable<AttributesExtractor<ProducerRecord<?, ?>, Void>> extractors) {

    KafkaProducerAttributesGetter getter = KafkaProducerAttributesGetter.INSTANCE;
    MessageOperation operation = MessageOperation.SEND;

    InstrumenterBuilder<ProducerRecord<?, ?>, Void> instrumenterBuilder = Instrumenter
        .<ProducerRecord<?, ?>, Void>builder(
            openTelemetry,
            instrumentationName,
            MessagingSpanNameExtractor.create(getter, operation))
        .addAttributesExtractor(MessagingAttributesExtractor.create(getter, operation))
        .addAttributesExtractors(extractors)
        .addAttributesExtractor(new KafkaProducerAdditionalAttributesExtractor())
        .setErrorCauseExtractor(errorCauseExtractor);
    if (!capturedProducerMessageHeaders.isEmpty()) {
      instrumenterBuilder
          .addAttributesExtractor(
              new KafkaHeadersAttributesExtractor<>(
                  capturedProducerMessageHeaders,
                  KafkaHeadersAttributesExtractor.PRODUCER_RECORD_HEADERS_GETTER
              ));
    }
    return instrumenterBuilder.newInstrumenter(SpanKindExtractor.alwaysProducer());
  }

  public Instrumenter<ConsumerRecords<?, ?>, Void> createConsumerReceiveInstrumenter() {
    KafkaReceiveAttributesGetter getter = KafkaReceiveAttributesGetter.INSTANCE;
    MessageOperation operation = MessageOperation.RECEIVE;

    return Instrumenter.<ConsumerRecords<?, ?>, Void>builder(
            openTelemetry,
            instrumentationName,
            MessagingSpanNameExtractor.create(getter, operation))
        .addAttributesExtractor(MessagingAttributesExtractor.create(getter, operation))
        .setErrorCauseExtractor(errorCauseExtractor)
        .setEnabled(ExperimentalConfig.get().messagingReceiveInstrumentationEnabled())
        .newInstrumenter(SpanKindExtractor.alwaysConsumer());
  }

  public Instrumenter<ConsumerRecord<?, ?>, Void> createConsumerProcessInstrumenter() {
    return createConsumerOperationInstrumenter(MessageOperation.PROCESS, Collections.emptyList());
  }

  public Instrumenter<ConsumerRecord<?, ?>, Void> createConsumerOperationInstrumenter(
      MessageOperation operation,
      Iterable<AttributesExtractor<ConsumerRecord<?, ?>, Void>> extractors) {

    KafkaConsumerAttributesGetter getter = KafkaConsumerAttributesGetter.INSTANCE;

    InstrumenterBuilder<ConsumerRecord<?, ?>, Void> builder =
        Instrumenter.<ConsumerRecord<?, ?>, Void>builder(
                openTelemetry,
                instrumentationName,
                MessagingSpanNameExtractor.create(getter, operation))
            .addAttributesExtractor(MessagingAttributesExtractor.create(getter, operation))
            .addAttributesExtractor(new KafkaConsumerAdditionalAttributesExtractor())
            .addAttributesExtractors(extractors)
            .setErrorCauseExtractor(errorCauseExtractor);
    if (KafkaConsumerExperimentalAttributesExtractor.isEnabled()) {
      builder.addAttributesExtractor(new KafkaConsumerExperimentalAttributesExtractor());
    }
    if (!capturedConsumerMessageHeaders.isEmpty()) {
      builder
          .addAttributesExtractor(
              new KafkaHeadersAttributesExtractor<>(
                  capturedConsumerMessageHeaders,
                  KafkaHeadersAttributesExtractor.CONSUMER_RECORD_HEADERS_GETTER
              ));
    }

    if (!KafkaPropagation.isPropagationEnabled()) {
      return builder.newInstrumenter(SpanKindExtractor.alwaysConsumer());
    } else if (ExperimentalConfig.get().messagingReceiveInstrumentationEnabled()) {
      builder.addSpanLinksExtractor(
          SpanLinksExtractor.extractFromRequest(
              openTelemetry.getPropagators().getTextMapPropagator(),
              KafkaConsumerRecordGetter.INSTANCE));
      return builder.newInstrumenter(SpanKindExtractor.alwaysConsumer());
    } else {
      return builder.newConsumerInstrumenter(KafkaConsumerRecordGetter.INSTANCE);
    }
  }

  public Instrumenter<ConsumerRecords<?, ?>, Void> createBatchProcessInstrumenter() {
    KafkaBatchProcessAttributesGetter getter = KafkaBatchProcessAttributesGetter.INSTANCE;
    MessageOperation operation = MessageOperation.PROCESS;

    return Instrumenter.<ConsumerRecords<?, ?>, Void>builder(
            openTelemetry,
            instrumentationName,
            MessagingSpanNameExtractor.create(getter, operation))
        .addAttributesExtractor(MessagingAttributesExtractor.create(getter, operation))
        .addSpanLinksExtractor(
            new KafkaBatchProcessSpanLinksExtractor(
                openTelemetry.getPropagators().getTextMapPropagator()))
        .setErrorCauseExtractor(errorCauseExtractor)
        .newInstrumenter(SpanKindExtractor.alwaysConsumer());
  }
}

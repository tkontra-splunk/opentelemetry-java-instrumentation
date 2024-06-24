/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.kafka.internal;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.instrumentation.api.config.Config;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * This class is internal and is hence not for public use. Its APIs are unstable and can change at
 * any time.
 */
public class KafkaMessageCapturedHeadersUtil {
  private static final String CONSUMER_HEADERS_PROPERTY =
      "otel.instrumentation.kafka.capture-headers.consumer";
  private static final String PRODUCER_HEADERS_PROPERTY =
      "otel.instrumentation.kafka.capture-headers.producer";

  private static final List<String> consumerMessageHeaders;
  private static final List<String> producerMessageHeaders;

  static {
    Config config = Config.get();
    consumerMessageHeaders = config.getList(CONSUMER_HEADERS_PROPERTY, emptyList());
    producerMessageHeaders = config.getList(PRODUCER_HEADERS_PROPERTY, emptyList());
  }

  // these are naturally bounded because they only store keys listed in
  // otel.instrumentation.kafka.capture-headers.consumer and
  // otel.instrumentation.kafka.capture-headers.producer
  private static final ConcurrentMap<String, AttributeKey<String>> consumerKeysCache =
      new ConcurrentHashMap<>();
  private static final ConcurrentMap<String, AttributeKey<String>> producerKeysCache =
      new ConcurrentHashMap<>();

  static List<String> lowercase(List<String> names) {
    return unmodifiableList(
        names.stream().map(s -> s.toLowerCase(Locale.ROOT)).collect(Collectors.toList()));
  }

  static AttributeKey<String> requestAttributeKey(String headerName) {
    return consumerKeysCache.computeIfAbsent(headerName, n -> createKey("consumer", n));
  }

  static AttributeKey<String> responseAttributeKey(String headerName) {
    return producerKeysCache.computeIfAbsent(headerName, n -> createKey("producer", n));
  }

  private static AttributeKey<String> createKey(String type, String headerName) {
    String headerNormalized = headerName.replace('-', '_').replace(" ", "_");
    String key = "kafka." + type + ".header." + headerNormalized;
    return AttributeKey.stringKey(key);
  }

  private KafkaMessageCapturedHeadersUtil() {}

  public static List<String> getProducerMessageHeaders() {
    return producerMessageHeaders;
  }

  public static boolean shouldCaptureProducerMessageHeaders() {
    return !getProducerMessageHeaders().isEmpty();
  }

  public static List<String> getConsumerMessageHeaders() {
    return consumerMessageHeaders;
  }

  public static boolean shouldCaptureConsumerMessageHeaders() {
    return !getConsumerMessageHeaders().isEmpty();
  }
}

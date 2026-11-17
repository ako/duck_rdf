import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;

/** Produces like the Mendix runtime would: Java client defaults (idempotent, acks=all), JSON values, CloudEvents headers. */
public class KafkaProduceTest {
  static KafkaProducer<String, String> producer(String bootstrap, String compression) {
    Properties p = new Properties();
    p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    p.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compression);
    p.put(ProducerConfig.LINGER_MS_CONFIG, 5);
    p.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 20000);
    p.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000);
    return new KafkaProducer<>(p);
  }
  static ProducerRecord<String, String> event(String topic, String key, String json, String type) {
    ProducerRecord<String, String> r = new ProducerRecord<>(topic, key, json);
    r.headers().add(new RecordHeader("ce_specversion", "1.0".getBytes(StandardCharsets.UTF_8)));
    r.headers().add(new RecordHeader("ce_type", type.getBytes(StandardCharsets.UTF_8)));
    r.headers().add(new RecordHeader("ce_id", UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8)));
    return r;
  }
  public static void main(String[] a) throws Exception {
    String bootstrap = a[0];
    int n = Integer.parseInt(a[1]);
    try (KafkaProducer<String, String> p = producer(bootstrap, "gzip")) {
      List<Future<RecordMetadata>> sent = new ArrayList<>();
      for (int i = 0; i < n; i++) {
        String json = "{\"id\":" + i + ",\"customer\":\"c" + (i % 7) + "\",\"amount\":" + (i * 1.25) +
            ",\"placed\":\"2026-11-17T10:00:" + String.format("%02d", i % 60) + "Z\",\"lines\":[{\"sku\":\"A\",\"qty\":" + (i % 3 + 1) + "}],\"extra\":true}";
        sent.add(p.send(event("orders", "order-" + i, json, "OrderPlaced")));
      }
      sent.add(p.send(event("orders", "bad-json", "this is not json", "OrderPlaced")));
      sent.add(p.send(event("orders", "bad-type", "{\"id\":\"not-a-number\",\"customer\":\"x\"}", "OrderPlaced")));
      RecordMetadata last = null;
      for (Future<RecordMetadata> f : sent) last = f.get();
      System.out.println("orders: " + sent.size() + " acknowledged, last offset " + last.offset() + " partition " + last.partition());
    }
    try (KafkaProducer<String, String> p = producer(bootstrap, "zstd")) {
      RecordMetadata m = null;
      for (int i = 0; i < 10; i++) m = p.send(event("mendix.events", null, "{\"entity\":\"Sales.Order\",\"change\":\"update\",\"n\":" + i + "}", "EntityChanged")).get();
      System.out.println("mendix.events: last offset " + m.offset());
    }
    try (KafkaProducer<String, String> p = producer(bootstrap, "snappy")) {
      try { p.send(event("orders", "x", "{}", "t")).get(); System.out.println("snappy: unexpectedly accepted"); }
      catch (Exception e) { System.out.println("snappy refused: " + e.getCause().getClass().getSimpleName()); }
    }
  }
}

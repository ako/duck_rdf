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
  static String order(long id, long orderId, String customer, String price, boolean revised) {
    return "{\"id\":" + id + ",\"OrderId\":" + orderId + ",\"CustomerName\":\"" + customer + "\",\"Price\":\"" + price +
        "\",\"Order_Customer\":[7],\"PlacedOn\":1794940282450" + (revised ? ",\"Note\":\"rush\"" : "") + "}";
  }
  static String change(String op, String before, String after) {
    boolean revised = (after != null && after.contains("Note")) || (before != null && before.contains("Note"));
    String fields = "[{\"field\":\"id\",\"type\":\"int64\"},{\"field\":\"OrderId\",\"type\":\"int64\"},"
        + "{\"field\":\"CustomerName\",\"type\":\"string\"},"
        + "{\"field\":\"Price\",\"name\":\"org.apache.kafka.connect.data.Decimal\",\"parameters\":{\"connect.decimal.precision\":\"28\",\"scale\":\"8\"},\"type\":\"bytes\"},"
        + "{\"field\":\"Order_Customer\",\"items\":{\"type\":\"int64\"},\"type\":\"array\"},"
        + "{\"field\":\"PlacedOn\",\"name\":\"org.apache.kafka.connect.data.Timestamp\",\"type\":\"int64\"}"
        + (revised ? ",{\"field\":\"Note\",\"type\":\"string\"}" : "") + "]";
    String schema = "{\"type\":\"struct\",\"name\":\"Order\",\"fields\":[{\"field\":\"before\",\"type\":\"struct\",\"optional\":true,\"fields\":" + fields
        + "},{\"field\":\"after\",\"type\":\"struct\",\"optional\":true,\"fields\":" + fields + "},{\"field\":\"op\",\"type\":\"string\"}]}";
    String payload = "{\"before\":" + before + ",\"after\":" + after + ",\"op\":\"" + op + "\",\"ts_ms\":1794940282647,"
        + "\"source\":{\"connector\":\"mendix_event_broker_cdc\",\"name\":\"mendix\",\"snapshot\":false,\"sequence\":1,\"ts_ms\":1794940282450,\"txId\":\"tx-1\",\"version\":\"11.16.1\"}}";
    return "{\"schema\":" + schema + ",\"payload\":" + payload + "}";
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
      for (int i = 0; i < 10; i++) {
        ProducerRecord<String, String> r = event("mendix.events", null, "{\"entity\":\"Sales.Order\",\"change\":\"update\",\"n\":" + i + "}", "EntityChanged");
        r.headers().add(new RecordHeader("ce_source", "/TestApp/EventsService/".getBytes(StandardCharsets.UTF_8)));
        m = p.send(r).get();
      }
      System.out.println("mendix.events: last offset " + m.offset());
    }
    if (n == 1000) {
      // Change events as the Mendix runtime's CDC publishes them (Debezium envelope, Kafka Connect
      // JSON with the schema embedded): create two orders, update one, delete the other, then one
      // more create of a later revision that adds an attribute.
      try (KafkaProducer<String, String> p = producer(bootstrap, "none")) {
        RecordMetadata m = null;
        String t = "cdc.TestApp.Order.1.local";
        m = p.send(new ProducerRecord<>(t, "11", change("c", null, order(11, 101, "Ada", "O05+wA==", false)))).get();
        m = p.send(new ProducerRecord<>(t, "12", change("c", null, order(12, 102, "Bob", "dzWUAA==", false)))).get();
        m = p.send(new ProducerRecord<>(t, "11", change("u", order(11, 101, "Ada", "O05+wA==", false), order(11, 101, "Ada", "SoF8gA==", false)))).get();
        m = p.send(new ProducerRecord<>(t, "12", change("d", order(12, 102, "Bob", "dzWUAA==", false), null))).get();
        Thread.sleep(2500); // committed first: the new attribute then adds a column to the table
        m = p.send(new ProducerRecord<>(t, "13", change("c", null, order(13, 103, "Cy", "/1aJMA==", true)))).get();
        System.out.println("cdc: last offset " + m.offset());
      }
    }
    try (KafkaProducer<String, String> p = producer(bootstrap, "snappy")) {
      try { p.send(event("orders", "x", "{}", "t")).get(); System.out.println("snappy: unexpectedly accepted"); }
      catch (Exception e) { System.out.println("snappy refused: " + e.getCause().getClass().getSimpleName()); }
    }
  }
}

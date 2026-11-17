import java.net.URI;
import java.util.*;
import org.apache.olingo.client.api.ODataClient;
import org.apache.olingo.client.api.communication.response.ODataRetrieveResponse;
import org.apache.olingo.client.api.domain.*;
import org.apache.olingo.client.core.ODataClientFactory;
import org.apache.olingo.commons.api.edm.*;

/** The OData endpoint through Apache Olingo's v4 client: metadata model, queries, navigation. */
public class ODataTest {
  static int failures = 0;
  static void check(String label, boolean ok, Object detail) {
    System.out.println((ok ? "PASS " : "FAIL ") + label + "  " + String.valueOf(detail).replace("\n", " | "));
    if (!ok) failures++;
  }
  interface Body { Object run() throws Exception; }
  static void attempt(String label, Body body, java.util.function.Predicate<Object> ok) {
    try { Object r = body.run(); check(label, ok.test(r), r); }
    catch (Exception e) { check(label, false, e.getClass().getSimpleName() + ": " + e.getMessage()); }
  }
  static Object prop(ClientEntity e, String name) {
    ClientProperty p = e.getProperty(name);
    return p == null || p.hasNullValue() ? null : p.getPrimitiveValue().toValue();
  }
  public static void main(String[] a) throws Exception {
    String base = a[0];
    ODataClient client = ODataClientFactory.getClient();
    client.getConfiguration().setDefaultPubFormat(org.apache.olingo.commons.api.format.ContentType.JSON);
    Edm edm = client.getRetrieveRequestFactory().getMetadataRequest(base).execute().getBody();
    EdmEntityContainer container = edm.getEntityContainer();
    attempt("metadata: entity sets", () -> { List<String> names = new ArrayList<>(); for (EdmEntitySet s : container.getEntitySets()) names.add(s.getName()); Collections.sort(names); return names; },
        r -> r.toString().equals("[Company, Employee, Employer, Order, Party, Payment, Person]"));
    attempt("metadata: Employee's typed properties and key", () -> { EdmEntityType t = container.getEntitySet("Employee").getEntityType();
        return t.getKeyPredicateNames() + " salary:" + t.getStructuralProperty("salary").getType().getName() + " worksFor->" + t.getNavigationProperty("worksFor").getType().getName(); },
        r -> r.equals("[ID] salary:Int64 worksFor->Company"));
    attempt("metadata: Payment.amount is Decimal, Order.total is Double", () -> container.getEntitySet("Payment").getEntityType().getStructuralProperty("amount").getType().getName()
        + "/" + container.getEntitySet("Order").getEntityType().getStructuralProperty("total").getType().getName(), r -> r.equals("Decimal/Double"));

    attempt("query: $filter, $orderby, $select, $count", () -> {
        URI uri = client.newURIBuilder(base).appendEntitySetSegment("Employee").filter("salary gt 50000").orderBy("salary desc").select("name,salary").count(true).build();
        ClientEntitySet set = client.getRetrieveRequestFactory().getEntitySetRequest(uri).execute().getBody();
        List<String> out = new ArrayList<>(); for (ClientEntity e : set.getEntities()) out.add(prop(e, "name") + ":" + prop(e, "salary"));
        return set.getCount() + " " + out; }, r -> r.equals("2 [Bob:61000, Ann:52000]"));
    attempt("query: string functions and $top/$skip", () -> {
        URI uri = client.newURIBuilder(base).appendEntitySetSegment("Employee").filter("contains(tolower(name),'b') or startswith(name,'C')").orderBy("name").top(1).skip(1).build();
        ClientEntitySet set = client.getRetrieveRequestFactory().getEntitySetRequest(uri).execute().getBody();
        return prop(set.getEntities().get(0), "name"); }, r -> "Cy".equals(r));
    attempt("query: filter on a navigation property", () -> {
        URI uri = client.newURIBuilder(base).appendEntitySetSegment("Employee").filter("worksFor/name eq 'Acme'").orderBy("name").build();
        List<Object> out = new ArrayList<>(); for (ClientEntity e : client.getRetrieveRequestFactory().getEntitySetRequest(uri).execute().getBody().getEntities()) out.add(prop(e, "name"));
        return out; }, r -> r.toString().equals("[Ann, Cy]"));
    attempt("entity by key", () -> {
        URI uri = client.newURIBuilder(base).appendEntitySetSegment("Employee").appendKeySegment("http://data.example.com/person/11").build();
        ClientEntity e = client.getRetrieveRequestFactory().getEntityRequest(uri).execute().getBody();
        return prop(e, "name") + " " + prop(e, "salary"); }, r -> r.equals("Bob 61000"));
    attempt("navigation: Employee(key)/worksFor", () -> {
        URI uri = client.newURIBuilder(base).appendEntitySetSegment("Employee").appendKeySegment("http://data.example.com/person/11").appendNavigationSegment("worksFor").build();
        return prop(client.getRetrieveRequestFactory().getEntityRequest(uri).execute().getBody(), "name"); }, r -> "Globex".equals(r));
    attempt("$expand with a nested $select", () -> {
        URI uri = client.newURIBuilder(base).appendEntitySetSegment("Order").expandWithSelect("orderedBy", "name").orderBy("total").build();
        List<String> out = new ArrayList<>();
        for (ClientEntity e : client.getRetrieveRequestFactory().getEntitySetRequest(uri).execute().getBody().getEntities()) {
          ClientLink link = e.getNavigationLink("orderedBy");
          ClientEntity target = link == null ? null : link.asInlineEntity().getEntity();
          out.add(prop(e, "total") + "->" + (target == null ? null : prop(target, "name")));
        }
        return out; }, r -> r.toString().equals("[7.25->Bob, 9.5->Ann, 20.0->Ann]"));
    attempt("$count segment", () -> {
        URI uri = client.newURIBuilder(base).appendEntitySetSegment("Party").count().build();
        return client.getRetrieveRequestFactory().getValueRequest(uri).execute().getBody().toString(); }, r -> r.equals("5"));
    attempt("decimal values", () -> {
        URI uri = client.newURIBuilder(base).appendEntitySetSegment("Payment").orderBy("amount").build();
        List<Object> out = new ArrayList<>(); for (ClientEntity e : client.getRetrieveRequestFactory().getEntitySetRequest(uri).execute().getBody().getEntities()) out.add(prop(e, "amount"));
        return out; }, r -> r.toString().equals("[0.1, 0.2]"));
    attempt("unknown property in $filter is a 400", () -> {
        URI uri = client.newURIBuilder(base).appendEntitySetSegment("Employee").filter("nosuch eq 1").build();
        try { client.getRetrieveRequestFactory().getEntitySetRequest(uri).execute(); return "no error"; }
        catch (org.apache.olingo.client.api.communication.ODataClientErrorException e) { return e.getStatusLine().getStatusCode(); } },
        r -> Integer.valueOf(400).equals(r));
    System.out.println(failures + " failure(s)");
    System.exit(failures == 0 ? 0 : 1);
  }
}

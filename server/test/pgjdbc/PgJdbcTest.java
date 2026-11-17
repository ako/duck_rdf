import java.math.BigDecimal;
import java.sql.*;
import java.util.*;

/** What Mendix's external database connector does over the PostgreSQL JDBC driver. */
public class PgJdbcTest {
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
  static List<List<Object>> rows(ResultSet r) throws SQLException {
    List<List<Object>> out = new ArrayList<>();
    int n = r.getMetaData().getColumnCount();
    while (r.next()) { List<Object> row = new ArrayList<>(); for (int i = 1; i <= n; i++) row.add(r.getObject(i)); out.add(row); }
    return out;
  }
  public static void main(String[] a) throws Exception {
    String url = "jdbc:postgresql://127.0.0.1:" + a[0] + "/lake?sslmode=disable";
    try { DriverManager.getConnection(url, "dev", "nope"); check("wrong password is refused", false, "connected"); }
    catch (SQLException e) { check("wrong password is refused", "28P01".equals(e.getSQLState()), e.getMessage()); }
    try (Connection c = DriverManager.getConnection(url, "dev", a[1])) {
      DatabaseMetaData md = c.getMetaData();
      attempt("metadata: product and version", () -> md.getDatabaseProductName() + " " + md.getDatabaseProductVersion(), r -> true);
      attempt("metadata: getTables", () -> { List<String> t = new ArrayList<>();
          try (ResultSet r = md.getTables(null, null, "%", new String[]{"TABLE", "VIEW"})) { while (r.next()) t.add(r.getString("TABLE_SCHEM") + "." + r.getString("TABLE_NAME") + ":" + r.getString("TABLE_TYPE")); } return t; },
          r -> r.toString().contains("emp") && r.toString().contains("payment"));
      attempt("metadata: getColumns of emp", () -> { List<String> t = new ArrayList<>();
          try (ResultSet r = md.getColumns(null, null, "emp", "%")) { while (r.next()) t.add(r.getString("COLUMN_NAME") + ":" + r.getString("TYPE_NAME") + ":" + r.getInt("DATA_TYPE")); } return t; },
          r -> r.toString().contains("name") && r.toString().contains("salary"));
      attempt("metadata: getPrimaryKeys", () -> rows(md.getPrimaryKeys(null, null, "emp")), r -> true);
      attempt("metadata: getSchemas", () -> rows(md.getSchemas()), r -> true);
      attempt("metadata: getTypeInfo", () -> rows(md.getTypeInfo()).size(), r -> ((Integer) r) > 0);

      Statement s = c.createStatement();
      attempt("simple query: join", () -> rows(s.executeQuery("SELECT e.name, c.name FROM emp e JOIN company c ON e.company_id = c.id ORDER BY 1")),
          r -> r.toString().equals("[[Ann, Acme], [Bob, Globex], [Cy, Acme]]"));
      attempt("simple query: types", () -> { ResultSet r = s.executeQuery("SELECT 1::INT i, 2::BIGINT b, 1.5::DOUBLE d, 12.34::DECIMAL(10,2) n, true t, DATE '2024-02-29' dt, TIMESTAMP '2024-01-02 03:04:05.123' ts, 'x' v, NULL::INT z");
          r.next(); return Arrays.asList(r.getInt(1), r.getLong(2), r.getDouble(3), r.getBigDecimal(4), r.getBoolean(5), r.getDate(6), r.getTimestamp(7), r.getString(8), r.getObject(9)); },
          r -> r.toString().equals("[1, 2, 1.5, 12.34, true, 2024-02-29, 2024-01-02 03:04:05.123, x, null]"));
      attempt("SPARQL through SQL", () -> rows(s.executeQuery("SELECT * FROM sparql('SELECT ?n ?s WHERE { ?p <http://example.com/ns#name> ?n ; <http://example.com/ns#salary> ?s } ORDER BY ?n')")),
          r -> r.toString().equals("[[Ann, 52000], [Bob, 61000], [Cy, 48000]]"));

      PreparedStatement p = c.prepareStatement("SELECT name, salary FROM emp WHERE id = ?");
      attempt("prepared: int parameter, 10 runs (server-side prepare, binary transfer)", () -> { List<Object> out = new ArrayList<>();
          for (int i = 0; i < 10; i++) { p.setInt(1, 10 + (i % 3)); try (ResultSet r = p.executeQuery()) { r.next(); out.add(r.getString(1) + "/" + r.getInt(2)); } } return out; },
          r -> r.toString().startsWith("[Ann/52000, Bob/61000, Cy/48000, Ann/52000"));
      PreparedStatement ps = c.prepareStatement("SELECT id FROM emp WHERE name = ? OR salary > ? ORDER BY id");
      attempt("prepared: string and decimal parameters, 7 runs", () -> { Object last = null;
          for (int i = 0; i < 7; i++) { ps.setString(1, "Cy"); ps.setBigDecimal(2, new BigDecimal("60000.50")); last = rows(ps.executeQuery()); } return last; },
          r -> r.toString().equals("[[11], [12]]"));
      PreparedStatement pn = c.prepareStatement("SELECT count(*) FROM emp WHERE ? IS NULL OR name = ?");
      attempt("prepared: null parameter", () -> { pn.setNull(1, Types.VARCHAR); pn.setString(2, "x"); return rows(pn.executeQuery()); },
          r -> r.toString().equals("[[3]]"));
      PreparedStatement pd = c.prepareStatement("SELECT ?::DATE + 1, ?::TIMESTAMP");
      attempt("prepared: date and timestamp parameters", () -> { Object last = null;
          for (int i = 0; i < 6; i++) { pd.setDate(1, java.sql.Date.valueOf("2024-02-28")); pd.setTimestamp(2, Timestamp.valueOf("2024-01-02 03:04:05")); last = rows(pd.executeQuery()); } return last; },
          r -> r.toString().contains("2024-02-29") && r.toString().contains("2024-01-02 03:04:05"));
      PreparedStatement psp = c.prepareStatement("SELECT * FROM sparql(?)");
      attempt("prepared: SPARQL query as a parameter", () -> { psp.setString(1, "SELECT ?n WHERE { ?p <http://example.com/ns#name> ?n } ORDER BY ?n"); return rows(psp.executeQuery()); },
          r -> r.toString().contains("Ann"));

      attempt("error, then the connection still works", () -> {
          try { s.executeQuery("SELECT * FROM nosuchtable"); return "no error"; } catch (SQLException e) { }
          return rows(s.executeQuery("SELECT count(*) FROM emp")); }, r -> r.toString().equals("[[3]]"));

      attempt("transaction with a fetch size (portal suspension)", () -> {
          c.setAutoCommit(false); Statement f = c.createStatement(); f.setFetchSize(2);
          int n = 0; try (ResultSet r = f.executeQuery("SELECT range FROM range(7)")) { while (r.next()) n++; }
          c.commit(); c.setAutoCommit(true); return n; }, r -> r.equals(7));
      attempt("isolation level and read-only (session commands)", () -> { c.setReadOnly(false); return c.getTransactionIsolation(); }, r -> true);
      attempt("batch of statements in one simple query", () -> { s.execute("SELECT 1; SELECT 2"); return "ok"; }, r -> true);
    }
    System.out.println(failures + " failure(s)");
    System.exit(failures == 0 ? 0 : 1);
  }
}

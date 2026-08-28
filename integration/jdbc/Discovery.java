import java.sql.*;
import java.util.*;

/** Optional real-driver gate. No build tool or Java dependency in the server.
 * Run with PostgreSQL JDBC 42.7.3 on the classpath and a local RF3 JDBC URL.
 * Read-only unless --writes is passed; writes touch only a unique test key.
 */
public class Discovery {
  interface Probe { Object run() throws Exception; }
  static int passed, failed;
  static void check(boolean ok, String why) { if (!ok) throw new IllegalStateException(why); }
  static void probe(String name, Probe p) {
    try {
      Object result = p.run();
      if (result instanceof ResultSet rs) {
        try (rs) {
          int n = 0;
          while (rs.next()) {
            for (int i=1; i<=rs.getMetaData().getColumnCount(); i++) rs.getString(i);
            n++;
          }
          result = "rows=" + n;
        }
      }
      System.out.println("PASS " + name + " " + result); passed++;
    } catch (Exception e) { System.out.println("FAIL " + name + " " + e); failed++; }
  }
  public static void main(String[] args) throws Exception {
    if (args.length == 0) throw new IllegalArgumentException("JDBC URL required; optionally --writes");
    try (Connection c=DriverManager.getConnection(args[0])) {
      DatabaseMetaData m=c.getMetaData();
      probe("driver", () -> m.getDriverName()+" "+m.getDriverVersion());
      probe("connection", () -> {check(c.isValid(5),"invalid connection"); return true;});
      probe("isolation", () -> {check(c.getTransactionIsolation()==Connection.TRANSACTION_READ_COMMITTED,"default isolation");return "read committed";});
      probe("catalog", c::getCatalog);
      probe("schema", () -> {check("public".equals(c.getSchema()),"schema");return c.getSchema();});
      probe("catalogs", m::getCatalogs);
      probe("schemas", m::getSchemas);
      probe("public schemas", () -> m.getSchemas(null,"public"));
      probe("tables", () -> m.getTables(null,null,"%",null));
      probe("public tables", () -> m.getTables(null,"public","%",new String[]{"TABLE"}));
      probe("table types", m::getTableTypes);
      probe("columns", () -> m.getColumns(null,"public","%","%"));
      probe("document columns", () -> {
        Set<String> names=new HashSet<>();
        try(ResultSet r=m.getColumns(null,"public","documents","%")) {
          while(r.next()) {names.add(r.getString("COLUMN_NAME"));check("json".equals(r.getString("TYPE_NAME")),"JSON projection type");}
        }
        check(names.containsAll(List.of("id","$doc")),"missing key/document columns"); return names;
      });
      probe("primary keys", () -> {try(ResultSet r=m.getPrimaryKeys(null,"public","documents")){check(r.next()&&"id".equals(r.getString("COLUMN_NAME"))&&!r.next(),"primary key");}return "id";});
      probe("indexes", () -> m.getIndexInfo(null,"public","documents",false,true));
      probe("unique indexes", () -> m.getIndexInfo(null,"public","documents",true,false));
      probe("imported keys", () -> m.getImportedKeys(null,"public","documents"));
      probe("exported keys", () -> m.getExportedKeys(null,"public","documents"));
      probe("best row identifier", () -> m.getBestRowIdentifier(null,"public","documents",DatabaseMetaData.bestRowSession,true));
      // PgJDBC constructs xmin locally even when the server has no xid type.
      // Exercise the call, but do not advertise that client row as supported.
      probe("version columns (driver-generated)", () -> m.getVersionColumns(null,"public","documents"));
      probe("table privileges", () -> m.getTablePrivileges(null,"public","%"));
      probe("column privileges", () -> m.getColumnPrivileges(null,"public","documents","%"));
      probe("types", m::getTypeInfo);
      probe("user types", () -> m.getUDTs(null,"public","%",null));
      probe("functions", () -> m.getFunctions(null,"public","%"));
      probe("function columns", () -> m.getFunctionColumns(null,"public","%","%"));
      probe("procedures", () -> m.getProcedures(null,"public","%"));
      probe("procedure columns", () -> m.getProcedureColumns(null,"public","%","%"));
      probe("client info", m::getClientInfoProperties);
      probe("GoLand search path", () -> {
        try(Statement s=c.createStatement();ResultSet r=s.executeQuery("select current_database() as a, current_schemas(false) as b")) {
          check(r.next(),"search path row");
          Array a=r.getArray(2);
          try {check(Arrays.equals((Object[])a.getArray(),new Object[]{"public"}),"search path array");} finally {a.free();}
        }
        return "public";
      });
      probe("GoLand data grid read", () -> {
        try(Statement s=c.createStatement();ResultSet r=s.executeQuery("SELECT t.* FROM public.documents t LIMIT 501")) {
          int n=0; while(r.next()) {r.getString(1);n++;} return n;
        }
      });
      probe("key and qualified whole document", () -> {
        try(Statement s=c.createStatement();ResultSet r=s.executeQuery("select id,documents.\"$doc\" from documents")) {
          check("$doc".equals(r.getMetaData().getColumnLabel(2)),"document label");
          int n=0;while(r.next()){check(r.getString(1)!=null&&r.getString(2).contains("\"id\""),"whole document");n++;}return n;
        }
      });
      probe("missing field is SQL NULL", () -> {
        try(Statement s=c.createStatement();ResultSet r=s.executeQuery("select id,definitely_absent_discovery_field from documents")) {
          int n=0;while(r.next()){check(r.getString(2)==null&&r.wasNull(),"missing value");n++;}return n;
        }
      });
      probe("JSON text access and prepared filter", () -> {
        try(PreparedStatement p=c.prepareStatement("SELECT documents.\"$doc\"->>'id' AS id_text, \"$doc\"->>'definitely_absent_discovery_field' FROM public.documents WHERE \"$doc\"->>'id' = ?")) {
          for(String id:List.of("local-smoke","no-such-json-access-probe")) {
            p.setString(1,id);
            try(ResultSet r=p.executeQuery()) {
              check(r.getMetaData().getColumnType(1)==Types.VARCHAR,"JSON text metadata");
              while(r.next()) check(id.equals(r.getString(1))&&r.getString(2)==null&&r.wasNull(),"JSON text/null/filter");
            }
          }
        }
        return "text metadata, bound filter, SQL NULL";
      });
      if (Arrays.asList(args).contains("--writes")) probe("prepared public CRUD", () -> {
        String id="jdbc-discovery-"+UUID.randomUUID();
        try {
          try(PreparedStatement p=c.prepareStatement("INSERT INTO public.documents (id,value) VALUES (?,?)")) {
            p.setString(1,id);p.setString(2,"before");check(p.executeUpdate()==1,"insert count");
          }
          try(PreparedStatement p=c.prepareStatement("UPDATE public.documents SET \"$doc\"=? WHERE id=?")) {
            p.setString(1,"{\"id\":\""+id+"\",\"value\":\"after\"}");p.setString(2,id);check(p.executeUpdate()==1,"update count");
          }
          try(PreparedStatement p=c.prepareStatement("SELECT value FROM public.documents WHERE id=?")) {
            p.setString(1,id);try(ResultSet r=p.executeQuery()){check(r.next()&&"\"after\"".equals(r.getString(1))&&!r.next(),"read after write");}
          }
          try(PreparedStatement p=c.prepareStatement("SELECT \"$doc\"->>'value' FROM public.documents WHERE id=?")) {
            p.setString(1,id);try(ResultSet r=p.executeQuery()){check(r.next()&&"after".equals(r.getString(1))&&!r.next(),"JSON text read after write");}
          }
        } finally {
          try(PreparedStatement p=c.prepareStatement("DELETE FROM public.documents WHERE id=?")){p.setString(1,id);p.executeUpdate();}
        }
        try(PreparedStatement p=c.prepareStatement("SELECT id FROM public.documents WHERE id=?")){p.setString(1,id);try(ResultSet r=p.executeQuery()){check(!r.next(),"delete visibility");}}
        return "insert/update/read/delete";
      });
    }
    System.out.println("SUMMARY passed="+passed+" failed="+failed);
    if(failed!=0) throw new IllegalStateException("JDBC discovery failed");
  }
}

import java.sql.*;
import java.nio.file.*;
import java.util.*;

/** Real GoLand-driver verification. Optional second argument is the repository's
 * employees-1000.sql fixture; seed only an empty table, never retry an unknown
 * write or overwrite existing rows. No Java dependency in the server. */
public class Employees {
  static void check(boolean ok, String why) { if (!ok) throw new IllegalStateException(why); }
  static int count(Connection c) throws Exception {
    try(Statement s=c.createStatement();ResultSet r=s.executeQuery("SELECT COUNT(*) FROM employees")) {
      check(r.next(),"count missing"); return r.getInt(1);
    }
  }
  public static void main(String[] args) throws Exception {
    check(args.length>=1,"JDBC URL required; optionally employees-1000.sql path");
    try(Connection c=DriverManager.getConnection(args[0])) {
      try(Statement s=c.createStatement()) {
        s.executeUpdate("CREATE TABLE IF NOT EXISTS public.employees (id TEXT PRIMARY KEY, name TEXT NOT NULL, team TEXT NOT NULL, city TEXT, score INTEGER NOT NULL, active BOOLEAN NOT NULL)");
      }
      System.out.println("PASS CREATE TABLE IF NOT EXISTS, public schema");
      Set<String> names=new HashSet<>();
      try(ResultSet r=c.getMetaData().getColumns(null,"public","employees","%")) {
        while(r.next()) names.add(r.getString("COLUMN_NAME"));
      }
      check(names.containsAll(List.of("id","name","team","city","score","active")),"missing declared columns: "+names);
      System.out.println("PASS JDBC declared columns "+names);
      int before=count(c);
      if(args.length>1 && before!=1000) {
        boolean resume=args.length==3 && args[2].equals("--resume-confirmed-prefix="+before);
        check(before==0 || resume && before%64==0,"refusing to seed nonempty employees table: "+before);
        if(before>0) {
          // An operator must first verify that the durable outbox has settled.
          // Verify the exact committed prefix; never overwrite or reinsert it.
          try(Statement s=c.createStatement();ResultSet r=s.executeQuery("SELECT \"$doc\"->>'id' FROM employees ORDER BY id")) {
            int i=0;
            while(r.next()) check(String.format("employee-%04d",++i).equals(r.getString(1)),"non-fixture row or gap");
            check(i==before,"prefix changed");
          }
        }
        String script=Files.readString(Path.of(args[1])).replaceAll("(?m)^\\s*--.*$","");
        int inserted=0, batches=0;
        for(String sql:script.split(";")) {
          if(!sql.stripLeading().startsWith("INSERT INTO employees")) continue;
          if(batches*64<before) { batches++; continue; }
          try(PreparedStatement p=c.prepareStatement(sql)) {
            int n=p.executeUpdate(); inserted+=n; batches++;
            System.out.println("PASS INSERT batch="+batches+" rows="+n);
          }
        }
        check(inserted+before==1000 && batches==16,"fixture seed counts");
      }
      int rows=count(c);
      if(args.length>1) check(rows==1000,"expected 1000 employees, got "+rows);
      try(PreparedStatement p=c.prepareStatement("SELECT id,name,team,city,score,active FROM public.employees WHERE \"$doc\"->>'city' = ? LIMIT 5")) {
        p.setString(1,"Lisbon");
        try(ResultSet r=p.executeQuery()) {
          check(r.getMetaData().getColumnCount()==6,"projection width");
          int matched=0;
          while(r.next()) { for(int i=1;i<=6;i++) check(r.getString(i)!=null,"missing field"); matched++; }
          if(rows==1000) check(matched==5,"city filter");
        }
      }
      System.out.println("PASS six-column prepared SELECT and JSON city filter");
      System.out.println("PASS employees rows="+rows);
    }
  }
}

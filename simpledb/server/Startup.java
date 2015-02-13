package simpledb.server;


import simpledb.parse.Parser;
import simpledb.query.Plan;
import simpledb.query.Scan;
import simpledb.tx.Transaction;

public class Startup {
   public static void main(String args[]) throws Exception {
     try {
         // analogous to the driver
      SimpleDB.init("studentdb");
      
         // analogous to the connection
      Transaction tx = new Transaction();
      
         // analogous to the statement
      String qry = "select SName, DName "
      + "from DEPT, STUDENT "
      + "where MajorId = DId"; 
      Plan p = SimpleDB.planner().createQueryPlan(qry, tx);
      
         // analogous to the result set
      Scan s = p.open();
      
      System.out.println("Name\tMajor");
      while (s.next()) {
            String sname = s.getString("sname"); //SimpleDB stores field names
            String dname = s.getString("dname"); //in lower case
            System.out.println(sname + "\t" + dname);
         }
         s.close();
         tx.commit();


      }
      catch(Exception e) {
      }

   }
}


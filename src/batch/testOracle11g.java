package batch;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

public class testOracle11g 
{   
            
    public static void main(String[] args)
    {
        // control processing
        testOracle11g tO11g = new testOracle11g();
        tO11g.testConnection();
    }
    
    public void testConnection()
    {
        System.out.println("Hello");
        String url = "jdbc:oracle:thin:@localhost:1521:orcl";
        try
        {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            Connection conn = DriverManager.getConnection(url,"WMBT_OWNER","Neil#1");
            String SQL = "SELECT COUNT(*) No_Childids FROM CHILDIDS";
            CallableStatement cstmt = conn.prepareCall(SQL);
            cstmt.execute();
            ResultSet rs = cstmt.getResultSet();
            rs.next();
            long result = rs.getLong("NO_CHILDIDS");
            System.out.println(result);
            rs = null;
            conn.close();
        }
        catch(Exception e)
        {            
            System.out.println( "DB ERROR: Failed to open ebilling database connection : " + e.toString());
        } 
    }
    
}

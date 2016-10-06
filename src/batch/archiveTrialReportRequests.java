
package batch;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

public class archiveTrialReportRequests 
{    
    // class variables
    private String dbServer, username, password;
    private String logDir, pdfTrialDir, archivePeriod;
    private String message;
    private BufferedWriter logBW;
    private Connection conn;
    private sqlServerDB sDB;
    private boolean dbOK;
    // class constants    
    // Database values
    private final String GIVNREF = "givn_ref";
    // String constants
    private final String USCORE = "_";
    // File extensions
    private final String LOGEXT = "_log.txt";
    // Progress message
    //private final String SUCCESSMESSAGE = " : OK";
    //private final String FAILUREMESSAGE = " : FAILED";
    private final String EMPTYMESSAGE = "No outstanding trial report requests to archive";
    
    public static void main(String[] args)
    {
        // control processing
        archiveTrialReportRequests aTRR = new archiveTrialReportRequests();
        aTRR.control();
    }
    
    private void control()
    {
        try
        {
          FileReader properties = new FileReader("reportRequest.properties");
          BufferedReader buffer = new BufferedReader(properties);
          boolean eofproperties = false;
          String propline = buffer.readLine();
          String propname = propline.substring(0,8);
          int lineLength = propline.length();
          String propval = propline.substring(9,lineLength).trim();
          while (!eofproperties)
          {
            if (propname.equals("dbserver"))
              dbServer = propval;
            if (propname.equals("username"))
              username = propval;
            if (propname.equals("password"))
              password = propval;
            if (propname.equals("logdir  "))
              logDir = propval;
            if (propname.equals("pdftdir "))
              pdfTrialDir = propval;
            if (propname.equals("archdays"))
              archivePeriod = propval;
            propline = buffer.readLine();
            if (propline == null)
              eofproperties = true;
            else 
            {
              propname = propline.substring(0,8);
              lineLength = propline.length();
              propval = propline.substring(9,lineLength).trim();
            }
          }
        }
        catch (IOException e)
        {
          message = "Error accessing properties file --- " + e.toString();
          System.out.println(message);
        }  
        openLogFile();
        // open database connection
        String url = "jdbc:AvenirDriver://"+dbServer+":1433/"+GIVNREF;
        try
        {
            Class.forName("net.avenir.jdbcdriver7.Driver");
            conn = DriverManager.getConnection(url,username,password);
            sDB = new sqlServerDB(conn);
            dbOK = true;
            message = "Successfully connected to database";
            System.out.println(message);
            writeToLogFile(message);
        }
        catch(Exception e)
        {            
            message = "ERROR: Failed to open database connection : " + e.toString();
            System.out.println(message);
            writeToLogFile(message);
            dbOK = false;
        }
        // process report requests that are past the archive period
        if (dbOK)
        {   
            message = "   Archiving trial report request more than "+archivePeriod+" days old";
            System.out.println(message);
            writeToLogFile(message);
            int successCount = 0, failureCount = 0;
            ResultSet requestsRS = sDB.getArchiveReportRequests(archivePeriod);
            try
            {                
                while((requestsRS.next())&&(dbOK))
                {
                    String reportFilename = requestsRS.getString("Report_Filename").trim();
                    String reportRequestId = requestsRS.getString("Report_Request_Id").trim();
                    // check that the report exists
                    File reportRequestToArchive = new File(pdfTrialDir+File.separator+reportFilename);
                    if (reportRequestToArchive.exists())
                    {
                        if (reportRequestToArchive.delete())
                        {
                            // Update the report request status to archived
                            if (sDB.updateReportRequestForArchive(reportRequestId))
                                successCount++;
                            else
                            {
                                message = 
                                    "ERROR: Failed to update report request ("+
                                    reportFilename+
                                    ") id = "+
                                    reportRequestId;
                                System.out.println(message);
                                writeToLogFile(message);
                                failureCount++;
                                
                            }
                        } 
                        else
                        {
                            message = "ERROR: Failed to delete report request ("+reportFilename+") id = "+reportRequestId;
                            System.out.println(message);
                            writeToLogFile(message);
                            failureCount++;                            
                        }
                    }
                    else
                    {
                        message = "ERROR: Unable to find file "+reportFilename+" for report request id = "+reportRequestId;
                        System.out.println(message);
                        writeToLogFile(message);
                        failureCount++;
                    }
                }
            } 
            catch(java.sql.SQLException ex)
            {
                message = "   SQL DB ERROR: Processing trial report requests to archive : " + ex.getMessage();
                System.out.println(message);
                writeToLogFile(message);
                dbOK = false;
            }    
            // summary message
            int totalCount = successCount + failureCount;
            if (totalCount==0)
                message = "   "+EMPTYMESSAGE;
            else
                message = "   " + totalCount + " requests identified for archiving : " + successCount + " OK : " + failureCount + " FAILED";                
            System.out.println(message);
            writeToLogFile(message);
        }
        if (dbOK)            
        {
            // close database connection
            try
            {
                conn.close();
                message = "Successfully closed database connection";
                System.out.println(message);
                writeToLogFile(message);
            }
            catch(Exception e)
            {            
                message = "ERROR: Failed to close database connection : " + e.toString();
                System.out.println(message);
                writeToLogFile(message);
            }
        }
        // close log file
        closeLogFile();
    }
        
    // open log file
    private void openLogFile()
    {
        logBW = null;
        String logDate = new java.util.Date().toString();
        String logFilename = 
            logDir+File.separator+"archiveTrialReportRequests_"+
            logDate.substring(24,28)+decodeMonth(logDate.substring(4,7))+
            logDate.substring(8,10)+USCORE+logDate.substring(11,13)+
            logDate.substring(14,16)+logDate.substring(17,19)+
            LOGEXT;
        try
        {
            logBW = new BufferedWriter(new FileWriter(logFilename));
        }
        catch (IOException ex)
        {
            System.out.println("   Error opening log file : "+ ex.getMessage());
        }
    }

    // write line to log file
    private void writeToLogFile( String line )
    {
        try
        {
            logBW.write(line+"\r\n");
        }
        catch (IOException ex)
        {
            System.out.println("   Error writing to log file : "+  ex.getMessage());
        }
    }

    // close log file
    private void closeLogFile()
    {
        try
        {
            logBW.close();
        }
        catch (IOException ex)
        {
            System.out.println("   Error closing log file : "+ ex.getMessage());
        }
    }
    
    // work out month number from text value
    private String decodeMonth( String month)
    {
        String result = "";
        if (month.startsWith("Jan"))
            result = "01";
        else if (month.startsWith("Feb"))
            result = "02";
        else if (month.startsWith("Mar"))
            result = "03";
        else if (month.startsWith("Apr"))
            result = "04";
        else if (month.startsWith("May"))
            result = "05";
        else if (month.startsWith("Jun"))
            result = "06";
        else if (month.startsWith("Jul"))
            result = "07";
        else if (month.startsWith("Aug"))
            result = "08";
        else if (month.startsWith("Sep"))
            result = "09";
        else if (month.startsWith("Oct"))
            result = "10";
        else if (month.startsWith("Nov"))
            result = "11";
        else if (month.startsWith("Dec"))
            result = "12";
        return result;
    }
        
}

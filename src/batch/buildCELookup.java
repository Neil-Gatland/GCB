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

public class buildCELookup 
{
    // property values
    private String logDir, crystalDir, outDir; 
    // file IO variables
    private BufferedWriter logBW, lookupFileBW;
    // messaging variables
    private String message; 
    // String constants    
    private final String USCORE = "_";
    private final String COMMA = ",";
    private final String LOG = "log";
    private final String LOOKUPFILE = "CE_Lookup";
    // File extensions
    private final String TEXTEXT = ".txt";
    private final String RPTEXT = ".rpt";
    private final String CSVEXT = ".csv";
    private final String PDFEXT = ".pdf";
    // oracle connection variables
    private String dbServer, service, username, password;
    private Connection conn;
    // OracleDB class variables
    private oracleDB oDB;
    // global stored values
    long reportReference;
    long invoiceId;
    String reportType;
    String outputName;
    
    public static void main(String[] args)
    {
        buildCELookup bcl = new buildCELookup();
        bcl.control();
    }

    private void control()
    {
        boolean continueDB = true;
        // get relevant property values
        try
        {
          FileReader properties = new FileReader("buildCELookup.properties");   
          BufferedReader buffer = new BufferedReader(properties);
          boolean eofproperties = false;
          String propline = buffer.readLine();
          String propname = propline.substring(0,8);
          int lineLength = propline.length();
          String propval = propline.substring(9,lineLength).trim();
          while (!eofproperties)
          {
            if (propname.equals("logDir  "))
                logDir = propval;   
            if (propname.equals("CEDir   "))
                crystalDir = propval; 
            if (propname.equals("outDir  "))
                outDir = propval;
            if (propname.equals("dbserver"))
                dbServer = propval;
            if (propname.equals("service "))
                service = propval;
            if (propname.equals("username"))
                username = propval;
            if (propname.equals("password"))
                password = propval;
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
        openLookupFile();
        long idCount=0, reportCount=0, fileCount=0, dbCount=0, ticker=0;
        String time = new java.util.Date().toString().substring(11,19);
        outputMessage("Starting creation of CE Lookup  "+time);
        outputMessage(" ");
        // Clear down CE Lookup table
        if (createOracleObject())
        {
            if (!oDB.truncateCELookup())
            {
                outputMessage("Failed to cleardown CE_Lookup");
                continueDB = false;
            }
            destroyOracleObject();
        }
        // Process each folder and file from top level directory
        File topLevelDirectory = new File(crystalDir);
        File[] level1Array = topLevelDirectory.listFiles();
        for (int i = 0; i < level1Array.length; i++)
          {
            File level1File = level1Array[i];
            //outputMessage(" level1File: "+level1File.getName());
            if (level1File.isDirectory())
            {
               File[] level2Array = level1File.listFiles();
               for (int j=0; j < level2Array.length; j++)
               {
                   File level2File = level2Array[j];
                   //outputMessage("  level2File: " +level2File.getName());
                   if (level2File.isDirectory())
                   {
                       File[] level3Array = level2File.listFiles();
                       for (int k=0; k < level3Array.length; k++)
                       {
                           File level3File = level3Array[k];
                           //outputMessage("   level3File: " +level3File.getName());
                           if (level3File.isDirectory())
                           {
                               File[] level4Array = level3File.listFiles();
                               for (int l=0; l < level4Array.length; l++)
                               {
                                   File level4File = level4Array[l];
                                   String level4FileName = level4File.getName();
                                   //if ((level4File.isDirectory())&&(level4FileName.endsWith(RPTEXT)))
                                   if (level4File.isDirectory())
                                   {
                                       //outputMessage("    level4File: "+level4FileName);
                                       idCount++;
                                       File[] level5Array = level4File.listFiles();
                                       for (int m=0; m < level5Array.length; m++)
                                       {
                                           File level5File = level5Array[m];
                                           String level5FileName = level5File.getName();
                                           if ((level5File.isFile())&&(level5FileName.endsWith(RPTEXT)))
                                           {
                                               String level5FilePath = level5File.getPath();
                                               reportReference = Long.parseLong(level4FileName);
                                               // get invoice and report name from ebilling DB
                                               invoiceId = -1;
                                               reportType = "MISSING";
                                               outputName = "Not determined";
                                               if (continueDB)
                                               {
                                                  if (createOracleObject())
                                                  {
                                                       ResultSet InvoiceReportRS = oDB.getInvoiceReportDetails(reportReference);
                                                       try
                                                       {    
                                                           if (InvoiceReportRS.next())
                                                           {
                                                              invoiceId = InvoiceReportRS.getLong("Invoice_Id");
                                                              reportType = InvoiceReportRS.getString("Report_Type");
                                                              outputName = 
                                                                      String.valueOf(invoiceId)+
                                                                      USCORE+
                                                                      String.valueOf(reportReference)+
                                                                      USCORE+
                                                                      reportType.replaceAll(" ",USCORE);
                                                           }
                                                           InvoiceReportRS = null;
                                                        }
                                                        catch(java.sql.SQLException ex)
                                                        {
                                                            String ioMessage = ex.getMessage();
                                                            if (!ioMessage.startsWith("Io exception: Connection reset"))
                                                            {
                                                                message = "   DB ERROR: Failed to process Invoice Report Details "+
                                                                ex.getMessage(); 
                                                                outputMessage(message);
                                                            }
                                                        }
                                                       destroyOracleObject();
                                                   } 
                                               }
                                               reportCount++;
                                               ticker++;
                                               if (ticker>9999)
                                               {
                                                   System.out.println("10,000 reports processed");
                                                   ticker=0;
                                               }
                                               writeToLookupFile(
                                                level4FileName+COMMA+
                                                level5FilePath+COMMA+
                                                invoiceId+COMMA+
                                                reportType+COMMA+
                                                outputName+PDFEXT);
                                               fileCount++;
                                               if (continueDB)
                                               {
                                                   if (createOracleObject())
                                                   {
                                                       if (oDB.insertCELookup(
                                                               reportReference, 
                                                               level5FilePath, 
                                                               invoiceId, 
                                                               reportType, 
                                                               outputName))
                                                       {
                                                           dbCount++;
                                                       }
                                                       else
                                                       {
                                                            outputMessage("Failed to insert CE_Lookup");
                                                            continueDB = false;
                                                       }
                                                       destroyOracleObject();
                                                   }  
                                               }                                               
                                           }
                                       }
                                   }
                               }
                           }
                       }
                   }
               }
            }
          }
        time = new java.util.Date().toString().substring(11,19);
        outputMessage("   No. ids identified    : "+idCount);
        outputMessage("   No. reports found     : "+reportCount);
        outputMessage("   No. file writes       : "+fileCount);
        outputMessage("   No. database writes   : "+dbCount);
        outputMessage(" ");
        outputMessage("Completed creation of CE Lookup "+time);
        closeLookupFile();
        closeLogFile();
    } 
    
    private void outputMessage(String inMessage)
    {
        System.out.println(inMessage);
        writeToLogFile(inMessage);
    }   
     
    // open lookup file
    private void openLookupFile()
    {
        lookupFileBW = null;
        String lookupFileDate = new java.util.Date().toString();
        String lookupFileName = 
            outDir+
            File.separator+
            LOOKUPFILE+ 
            USCORE+   
            lookupFileDate.substring(24,28)+
            decodeMonth(lookupFileDate.substring(4,7))+
            lookupFileDate.substring(8,10)+
            USCORE+
            lookupFileDate.substring(11,13)+
            lookupFileDate.substring(14,16)+
            lookupFileDate.substring(17,19)+
            CSVEXT;
        try
        {
            lookupFileBW = new BufferedWriter(new FileWriter(lookupFileName));
        }
        catch (IOException ex)
        {
            outputMessage("   Error opening lookup file : "+ ex.getMessage());
        }
    }         
        
    // write line to lookup file
    private void writeToLookupFile( String line )
    {
        try
        {
            lookupFileBW.write(line+"\r\n");
        }
        catch (IOException ex)
        {
            outputMessage("   Error writing to lookup file : "+  ex.getMessage());
        }
    }  
    
    // close lookup file
    private void closeLookupFile()
    {
        try
        {
            lookupFileBW.close();
        }
        catch (IOException ex)
        {
            outputMessage("   Error closing lookup file : "+ ex.getMessage());
        }
    }   
    
    // open log file
    private void openLogFile()
    {
        logBW = null;
        String logDate = new java.util.Date().toString();
        String logFilename = 
            logDir+File.separator+"buildCELookup_"+
            logDate.substring(24,28)+
            decodeMonth(logDate.substring(4,7))+
            logDate.substring(8,10)+
            USCORE+
            logDate.substring(11,13)+
            logDate.substring(14,16)+
            logDate.substring(17,19)+
            USCORE+
            LOG+    
            TEXTEXT;
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
    
    // creates OracleDB object 
    private boolean createOracleObject()
    {
        boolean result = false;
        // open database connection
        String url = "jdbc:oracle:thin:@"+dbServer+":1521:"+service;
        try
        {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            conn = DriverManager.getConnection(url,username,password);
            oDB = new oracleDB(conn);
            result = true;
        }
        catch(Exception e)
        {            
            message = "DB ERROR: Failed to open ebilling database connection : " + e.toString();
            outputMessage(message);
            result = false;
        } 
        return result;
    }   
        
    // clears  OracleDB object
    private void destroyOracleObject()
    {
        try
        {
            conn.close();
            oDB = null;
        }
        catch(Exception e)
        {   
            String exMessage = e.toString();
            if (!exMessage.startsWith("java.sql.SQLException: Io exception: Connection reset by peer: socket write error"))
            {              
                message = "DB ERROR: Failed to close database connection : " + e.toString();
                outputMessage(message);
            }
        }        
    }
    
}

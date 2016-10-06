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

public class createListFiles 
{   
    // property values
    private String logDir, errorDir, listDir; 
    // file IO variables
    private BufferedWriter logBW, listBW;;
    private BufferedReader errorBR;
    private String errorFilename, listFilename;
    private Boolean errorEOF;
    // String constants    
    private final String USCORE = "_";
    private final String COMMA = ",";
    private final String LOG = "log";
    // File extensions
    private final String TEXTEXT = ".txt";
    // messaging variables
    private String message; 
    // oracle connection variables
    private String dbServer, service, username, password;
    private Connection conn;
    // OracleDB class variables
    private oracleDB oDB;
    // file count varibales
    private long errorCount, listCount;
    
    public static void main(String[] args)
    {
        createListFiles clf = new createListFiles();
        clf.control();
    }    
    
    private void control()
    {
        boolean continueDB = true;
        // get relevant property values
        try
        {
          FileReader properties = new FileReader("createListFiles.properties");   
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
            if (propname.equals("errorDir"))
                errorDir = propval; 
            if (propname.equals("listDir "))
                listDir = propval;
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
        String time = new java.util.Date().toString().substring(11,19);
        outputMessage("Starting creation of list file(s)  "+time);
        outputMessage(" "); 
        // Process each error file
        long errorFileCount = 0;
        File errorDirectory = new File(errorDir);
        File[] errorArray = errorDirectory.listFiles();
        for (int i = 0; i < errorArray.length; i++)
        {
           File errorFile = errorArray[i];
           errorFilename = errorFile.getName();
           outputMessage("  Processing error file "+errorFilename);
           errorCount=0;
           listCount=0;
           //System.out.println(errorFilename);
           listFilename = errorFilename.replaceAll("error", "list");
           //System.out.println(listFilename);
           errorFileCount++;
           // Read each line of error file
           openErrorFile();
           openListFile();
           String errorLine = getErrorFileLine();
           // process error file lines
           while(!errorEOF)
           {
                //System.out.println(errorLine);
                String listLine = errorLine;                
                errorCount++;
                if (errorLine!=null)
                {
                    // determine report reference from error line
                    long reportRef = getReportReference(errorLine), invoiceId = -1;
                    String reportName = "MISSING", reportLocation = "MISSING";
                    // get report name and location from CE_Lookup table
                    if (createOracleObject())
                    {
                        ResultSet ceLookupRS = oDB.getCELookupDetails(reportRef);
                        try
                        {
                            if (ceLookupRS.next())
                            {
                               invoiceId = ceLookupRS.getLong("Invoice_Id");
                               reportName = ceLookupRS.getString("Report_Name");
                               reportLocation = ceLookupRS.getString("Report_Location");                                
                            }
                            ceLookupRS = null;
                        }
                        catch(java.sql.SQLException ex)
                        {
                            String ioMessage = ex.getMessage();
                            if (!ioMessage.startsWith("Io exception: Connection reset"))
                            {
                                message = "   DB ERROR: Failed to process CE Lookup Details "+
                                ex.getMessage(); 
                                outputMessage(message);
                            }                                    
                        }
                        destroyOracleObject();
                    }
                    // format and write invoice list file line
                    if (invoiceId!=-1)
                    {
                        listCount++;
                        listLine = 
                                reportName.replaceAll(" ", USCORE)+COMMA+
                                reportLocation+COMMA+
                                invoiceId+USCORE+reportRef;
                        writeListFileLine(listLine);
                    }                   
                } 
                errorLine = getErrorFileLine();
           }
           outputMessage("    No. error records   : "+errorCount);
           outputMessage("    No. list records    : "+listCount);
           closeListFile();
           if (listCount==0)
           {
               File emptyFile = new File(listDir+File.separator+listFilename);
               if (emptyFile.delete())
                   outputMessage("  List file "+listFilename+" deleted as it is empty");
               else
                   outputMessage("  Failed to delete empty list file "+listFilename);               
           }
           else
              outputMessage("  List file "+listFilename+" created"); 
           outputMessage(" ");
           closeErrorFile();
        }
        time = new java.util.Date().toString().substring(11,19);
        outputMessage("No. Error files processed : "+errorFileCount);
        outputMessage(" "); 
        outputMessage("Completed creation of list file(s) "+time);
        closeLogFile();
    }   
    
    private long getReportReference(String errorFileLine)
    {
        long reportReference = -1;
        int scanPos = errorFileLine.length()-1;
        boolean notDone = true;
        String repRef = "";
        while(notDone)
        {
            String testChar = errorFileLine.substring(scanPos, scanPos+1);
            if (testChar.startsWith(USCORE))
                notDone = false;
            else
                repRef = testChar + repRef;
            scanPos--;
        }
        reportReference = Long.parseLong(repRef);
        return reportReference;
    }
        
    private void outputMessage(String inMessage)
    {
        System.out.println(inMessage);
        writeToLogFile(inMessage);
    } 
    
    // open error file 
    private void openErrorFile()
    {
        errorBR = null;
        try
        {
            errorBR = new BufferedReader(new FileReader(errorDir+File.separator+errorFilename));
            errorEOF = false;
        }
        catch(IOException ex)
        {
            System.out.println("   Error opening error file : "+errorFilename+" : "+ ex.getMessage());
        }
    }
    
    private String writeListFileLine(String line)
    {
        try
        {
            listBW.write(line+"\r\n");
        }        
        catch(IOException ex)
        {
            System.out.println("   Error writing to list file : "+listFilename+" : "+ ex.getMessage());
        }
        return line;        
    }
    
    // open error file 
    private void openListFile()
    {
        listBW = null;
        try
        {
            listBW = new BufferedWriter(new FileWriter(listDir+File.separator+listFilename));
        }
        catch(IOException ex)
        {
            System.out.println("   Error opening list file : "+listFilename+" : "+ ex.getMessage());
        }
    }   
    
    // close list file
    private void closeListFile()
    {
        try
        {
            listBW.close();
        }
        catch (IOException ex)
        {
            System.out.println("   Error closing list file : "+listFilename+" : "+ ex.getMessage());
        }
    } 
    
    private String getErrorFileLine()
    {
        String line ="";
        try
        {
            line = errorBR.readLine();
            if (line==null)
                errorEOF = true;
        }        
        catch(IOException ex)
        {
            System.out.println("   Error opening error file : "+errorFilename+" : "+ ex.getMessage());
        }
        return line;        
    }

    // close error file
    private void closeErrorFile()
    {
        try
        {
            errorBR.close();
        }
        catch (IOException ex)
        {
            System.out.println("   Error closing error file : "+errorFilename+" : "+ ex.getMessage());
        }
    } 
    
    // open log file
    private void openLogFile()
    {
        logBW = null;
        String logDate = new java.util.Date().toString();
        String logFilename = 
            logDir+File.separator+"Create_List_Files_"+
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

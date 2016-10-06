package batch;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

public class updateInProgressReportRequests 
{
    // class variables
    private String dbServer, username, password;
    private String logDir, pdfTrialDir, pdfClosedDir, transferDir, printDir;
    private String message;
    private BufferedWriter logBW, controlBW;
    private Connection conn;
    private sqlServerDB sDB;
    private boolean dbOK;
    private String lastInvoiceNo = "xxxxxxxxxxxxxxxxxxxx";
    // class constants    
    // Database values
    private final String GIVNREF = "givn_ref";
    // String constants
    private final String SINGLESPACE = " ";
    private final String COMMA = ",";
    private final String USCORE = "_";
    private final String PIPE = "|";
    // File extensions
    private final String LOGEXT = "_log.txt";
    private final String TXTEXT = ".txt";
    // File names
    private final String CONTROLFILENAME = "Transfer_Control_File";
    // Report name 
    private final String INVOICE = "Invoice";
    // Record types
    private final String CONTROLREC = "Control";
    private final String REPORTREC = "Report"; 
    // Progress message
    private final String SUCCESSMESSAGE = " : OK";
    private final String FAILUREMESSAGE = " : FAILED";
    private final String EMPTYMESSAGE = " No outstanding in progress requests to update";
    // Failure messages
    private final String PDFNOTFOUND = "PDF not produced";
    private final String ZIPNOTFOUND = "ZIP not produced";
    
    public static void main(String[] args)
    {
        // control processing
        updateInProgressReportRequests uIPRR = new updateInProgressReportRequests();
        uIPRR.control();
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
            if (propname.equals("pdfcdir "))
              pdfClosedDir = propval;
            if (propname.equals("xferdir "))
              transferDir = propval;
            if (propname.equals("printdir"))
              printDir = propval;
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
        // process report requests that are status In Progress
        if (dbOK)
        {
            ResultSet requestsRS = sDB.getInProgressReportRequests();
            boolean controlFileStarted = false;
            int successCount = 0, failureCount = 0;
            try
            {                
                while((requestsRS.next())&&(dbOK))
                {
                    long reportRequestId = requestsRS.getInt("Report_Request_Id");
                    String globalCustomerId = requestsRS.getString("Global_Customer_Id").trim();
                    String invoiceNo = requestsRS.getString("Invoice_No").trim();
                    String accountNumber = requestsRS.getString("Account_Number").trim();
                    String accountName = requestsRS.getString("Account_Name").trim();
                    String billingSource = requestsRS.getString("Billing_Source").trim();
                    String currencyCode = requestsRS.getString("ISO_Currency_Code").trim();
                    String taxPointDate = requestsRS.getString("Tax_Point_Date").trim();
                    String periodFromDate = requestsRS.getString("Period_From_Date").trim();
                    String periodToDate = requestsRS.getString("Period_To_Date").trim();
                    String invoiceTotal = requestsRS.getString("Invoice_Total");
                    String trialInd = requestsRS.getString("Trial_Ind").trim();
                    String reportName = requestsRS.getString("Report_Name").trim();
                    String displayName = requestsRS.getString("Display_Name").trim();
                    String reportType = requestsRS.getString("Report_Type");
                    String reportFilename = requestsRS.getString("Report_Filename").trim();
                    String printer = requestsRS.getString("Printer").trim();
                    String requestMessage = "   " +
                        globalCustomerId + COMMA + SINGLESPACE + 
                        invoiceNo + COMMA + SINGLESPACE +
                        reportName + SINGLESPACE;  
                    boolean OK = true;
                    if (pdfExists(trialInd,reportFilename))
                    {   
                        //If closed invoice report check if it can be copied for printing
                        if ((trialInd.startsWith("N"))&&(reportType.startsWith("Report")))                           
                            OK = checkForPrinting(reportFilename,printer);
                        //If closed invoice move report or export file to the transfer directory
                        if ((OK)&&(trialInd.startsWith("N")))                            
                            OK = moveInvoiceFile(reportFilename);
                        // If closed invoice report create invoice control line and report line
                        // otherwise just report line
                        if ((OK)&&(trialInd.startsWith("N")))                          
                        {
                            if (!controlFileStarted)
                            {
                                OK = openControlFile();
                                controlFileStarted = true;
                            }
                            //if ((OK)&&(reportName.endsWith(INVOICE)))
                            if ((OK)&&(!invoiceNo.startsWith(lastInvoiceNo)))
                            {
                                lastInvoiceNo=invoiceNo;
                                String controlFileLine =
                                    CONTROLREC + PIPE +
                                    globalCustomerId + PIPE +
                                    invoiceNo + PIPE +
                                    accountNumber + PIPE +
                                    accountName + PIPE +
                                    billingSource + PIPE +
                                    currencyCode + PIPE +
                                    taxPointDate + PIPE +
                                    periodFromDate + PIPE +
                                    periodToDate + PIPE +
                                    invoiceTotal;                                     
                                OK = writeToControlFile(controlFileLine);
                            }
                            if (OK)
                            {
                               String controlFileLine = REPORTREC + PIPE + reportFilename + PIPE + displayName;
                               OK = writeToControlFile(controlFileLine);
                            }
                        }
                        // update Request Status to Completed
                        // if closed also populate Transfer Flag
                        dbOK = sDB.updateInProgressReportRequest(reportRequestId,"N","",trialInd);
                        successCount++;
                        requestMessage = requestMessage + SUCCESSMESSAGE;
                    }
                    else
                    {
                        // Update Request Status to Failed and set Failure Message 
                        if (reportType.startsWith("Report"))
                            dbOK = sDB.updateInProgressReportRequest(reportRequestId,"Y",PDFNOTFOUND,trialInd);
                        else
                            dbOK = sDB.updateInProgressReportRequest(reportRequestId,"Y",ZIPNOTFOUND,trialInd);
                        failureCount++;
                        requestMessage = requestMessage + FAILUREMESSAGE;  
                    }                                           
                    // success / failure message
                    System.out.println(requestMessage);
                    writeToLogFile(requestMessage);
                }
            }
            catch(java.sql.SQLException ex)
            {
                message = "   SQL DB ERROR: Processing requested report requests : " + ex.getMessage();
                System.out.println(message);
                writeToLogFile(message);
                dbOK = false;
            }
            if (controlFileStarted)
                closeControlFile();
            requestsRS = null;
            // summary message
            int totalCount = successCount + failureCount;
            if (totalCount==0)
                message = "   "+EMPTYMESSAGE;
            else
                message = "   " + totalCount + " requests updated : " + successCount + " OK : " + failureCount + " FAILED";                
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
    
    private boolean pdfExists(String trialInd, String reportFilename)
    {
        boolean result = false;
        String pdfPath = File.separator + reportFilename;
        if (trialInd.startsWith("Y"))
            pdfPath = pdfTrialDir + pdfPath;
        else
            pdfPath = pdfClosedDir + pdfPath;
        File pdfFile = new File(pdfPath);
        if (pdfFile.exists())
            result = true;        
        return result;
    }
    
    // check if the report can be copied to a printer folder
    private boolean checkForPrinting(String reportFilename, String printer)
    {
        boolean result = true;
        // check if printer folder exists
        if (printer.length()>0)
        {
           String printFolderName = printDir + File.separator + printer;
           File printerFolder = new File(printFolderName);
           if (printerFolder.exists())
           {               
               String reportFileName = pdfClosedDir + File.separator + reportFilename;
               String printFileName =  printFolderName + File.separator + reportFilename;
               File reportFile = new File(reportFileName);
               File printFile = new File(printFileName);
               Path reportFilePath = reportFile.toPath();
               Path printFilePath = printFile.toPath();
               try
               {
                  Files.copy(reportFilePath, printFilePath, StandardCopyOption.REPLACE_EXISTING ); 
               }
               catch(IOException ex)
               {                 
                   message = 
                        "   I/O ERROR: Copying : " + 
                       reportFileName + " : to : " + 
                       printFileName + " : "+ 
                       ex.getMessage();
                   System.out.println(message);
                   writeToLogFile(message);  
                   result = false;
               }  
           }
        }
        return result;
    }   
    
    // move invoice report or export file to the transfer directory
    private boolean moveInvoiceFile(String reportFilename)
    {
        boolean result = true;
        File invoiceFile = new File(pdfClosedDir + File.separator + reportFilename);
        File copyFile = new File(transferDir + File.separator + reportFilename);
        if (copyFile.exists())
            if(!copyFile.delete())
                result = false;
        if (!invoiceFile.renameTo(copyFile))
            result = false;
        return result;
    }       
        
    // open batch script file
    private boolean openControlFile()
    {
        boolean result = false;
        String filename = 
            transferDir + File.separator + 
            CONTROLFILENAME + USCORE +
            fileDT() + TXTEXT;
        try
        {
           controlBW = new BufferedWriter(new FileWriter(filename)); 
           result = true;
        }
        catch(Exception ex)
        {
           message = "   I/O ERROR: Opening control file : " + filename + " : " + ex.getMessage();
           System.out.println(message);
           writeToLogFile(message);
        }
        return result;
    }
    
    // write line to batch script file
    private boolean writeToControlFile( String line )
    {
        boolean result = false;
        try
        {
            controlBW.write(line+"\r\n");
            result = true;
        }
        catch (IOException ex)
        {
           message = "   I/O ERROR: Writing to control file : " + ex.getMessage();
           System.out.println(message);
           writeToLogFile(message);
        }
        return result;
    }

    // close control file
    private void closeControlFile()
    {
        try
        {
            controlBW.close();
        }
        catch (IOException ex)
        {
           message = "   I/O ERROR: Closing control file : " + ex.getMessage();
           System.out.println(message);
           writeToLogFile(message);
        }
    }
        
    // open log file
    private void openLogFile()
    {
        logBW = null;
        String logDate = new java.util.Date().toString();
        String logFilename = 
            logDir+File.separator+"updateInProgressReportRequests_"+
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
    
    private String fileDT()
    {
        String DateTime = new java.util.Date().toString();
        String reformatDT =
          DateTime.substring(24,28)+
          decodeMonth(DateTime.substring(4,7))+
          DateTime.substring(8,10)+
          DateTime.substring(11,13)+
          DateTime.substring(14,16)+
          DateTime.substring(17,19);
        return reformatDT;
    }
}

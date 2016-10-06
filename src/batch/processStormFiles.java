package batch;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;

public class processStormFiles 
{
    
    // oracle connection variables
    private String dbServer, service, username, password;
    private Connection conn;
    // OracleDB class variables
    private oracleDB oDB;
    // file i/o variables
    private String logDir, dropDir, rejDir, attachDir;
    private BufferedWriter logBW; 
    // logging variables
    private String message;
    // String constants
    private final String USCORE = "_";
    private final String FWDSLASH = "/";
    private final String STORM = "Storm";
    private final String UNKNOWN = "UNKNOWN";
    private final String NOVALUE = "NOVALUE";
    // Attachment constants
    private final String DAILYCDR = "Storm Daily CDRs";
    private final String MONTHLYCDR = "Storm Monthly CDRs";
    private final String MONTHLYPDF = "Storm Invoice";
    private final String ACCOUNT = "account";
    private final String INVOICE = "invoice";
    private final String YYYYMMDD = "YYYYMMDD";
    private final String DDMONYY = "DD-MON-YY";
    private final String CDR = "cdr";
    private final String REPORT = "report";
    // File extensions
    private final String LOGEXT = "_log.txt";
    private final String PDFEXT = ".pdf";
    private final String ZIPEXT = ".zip";
    // File types
    private final String INVOICEPDF = "invoice";
    private final String CDRZIP = "cdr";
    private final String INVALID = "invalid";
    // CDR prefixes
    private final String DAILYPREFIX = "D";
    private final String MONTHLYPREFIX = "M";
    
    public static void main(String[] args)
    {
        // control processing
        processStormFiles psf = new processStormFiles();
        psf.control();
    }

    public void control()
    {
        message ="Storm file processing starting";
        System.out.println(message);
        boolean OK = true, logging = false;;
        try
        {
          FileReader properties = new FileReader("storm.properties");
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
            if (propname.equals("service "))
                service = propval;
            if (propname.equals("username"))
                username = propval;
            if (propname.equals("password"))
                password = propval;
            if (propname.equals("logdir  "))
                logDir = propval;
            if (propname.equals("dropdir "))
                dropDir = propval;
            if (propname.equals("rejdir  "))
                rejDir = propval;
            if (propname.equals("attchdir"))
                attachDir = propval;            
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
          message = "Error accessing storm properties file --- " + e.toString();
          System.out.println(message);
          OK = false;
        }
        // open log file
        if (OK)
        {
            OK = openLogFile();
            logging = true;
        }            
        if (OK)
            writeToLogFile(message);
        // process each file in the drop directory
        if (OK)
        {
            blankLine();
            int stormFileCount = 0;
            int invalidCount = 0, pdfCount = 0, dailyCDRCount = 0, monthlyCDRCount = 0;
            int invoiceSuccessCount = 0, invoiceFailCount = 0;
            int dailyCDRSuccessCount = 0, dailyCDRFailCount = 0;
            int monthlyCDRSuccessCount = 0, monthlyCDRFailCount = 0;
            File dropDirectory = new File(dropDir);
            File[] stormFileArray = dropDirectory.listFiles();
            for (int i = 0; i < stormFileArray.length; i++)
            {
                String stormFilename = stormFileArray[i].getName();
                //System.out.println(stormFilename);
                // Determine file type
                String fileType = INVALID;
                String CDRType = "";
                if (stormFilename.endsWith(PDFEXT))
                {
                    fileType = INVOICEPDF;
                    pdfCount++;
                }
                else if(stormFilename.endsWith(ZIPEXT))
                {
                    fileType = CDRZIP;
                    if (stormFilename.startsWith(DAILYPREFIX))
                    {
                        dailyCDRCount++;
                        CDRType = DAILYPREFIX;
                    }
                    else if (stormFilename.startsWith(MONTHLYPREFIX))
                    {
                        monthlyCDRCount++;
                        CDRType = MONTHLYPREFIX;
                    }
                    else
                    {
                        invalidCount++;
                        fileType = INVALID;
                    }
                } 
                if (fileType.equals(INVALID))
                {
                    message = "   File '" + stormFilename + "' is invalid and cannot be processed";
                    writeToLogFile(message);
                    System.out.println(message);
                    moveToReject(stormFilename);
                    invalidCount++;       
                }
                else
                {
                    // Get other required data from filename
                    String filenamePart[] = decodeFilename(stormFilename);
                    /*System.out.println(filenamePart[1]);
                    System.out.println(filenamePart[2]);
                    System.out.println(filenamePart[3]);
                    System.out.println(filenamePart[4]);
                    System.out.println(filenamePart[5]);
                    System.out.println(filenamePart[6]);*/
                    String fileDate = "", accountNumber = "", invoiceNumber = ""; 
                    boolean fileProcessed = true;
                    if (fileType.startsWith(INVOICEPDF))
                    {
                        fileDate = filenamePart[4]+filenamePart[5];
                        accountNumber = filenamePart[2];
                        invoiceNumber = filenamePart[1];
                        fileProcessed =
                            processInvoiceFile(
                                stormFilename,
                                fileDate,
                                accountNumber,
                                invoiceNumber);
                        if (fileProcessed)
                            invoiceSuccessCount++;
                        else
                            invoiceFailCount++;
                    }
                    else
                    {
                        if (CDRType.equals(DAILYPREFIX))
                        {
                            fileDate = filenamePart[2];                            
                            accountNumber = filenamePart[3];
                            fileProcessed =
                                processDailyCDRFile(
                                    stormFilename,
                                    fileDate,
                                    accountNumber);
                            if (fileProcessed)
                                dailyCDRSuccessCount++;
                            else
                                dailyCDRFailCount++;
                        }
                        else
                        {
                            fileDate = filenamePart[5]+filenamePart[6];
                            accountNumber = filenamePart[3];
                            invoiceNumber = filenamePart[2];
                            fileProcessed =
                                processMonthlyCDRFile(
                                    stormFilename,
                                    fileDate,
                                    accountNumber,
                                    invoiceNumber);
                            if (fileProcessed)
                                monthlyCDRSuccessCount++;
                            else
                                monthlyCDRFailCount++;
                        }
                    }
                }
                stormFileCount++;
            }
            if (stormFileCount==0)
            {
                message = "   No files in drop directory";
                writeToLogFile(message);
                System.out.println(message);
            }                       
            else
            {
                message = "   Files in drop directory = " + stormFileCount;
                writeToLogFile(message);
                System.out.println(message);
                if (invalidCount!=0)
                {
                    message = "      Invalid files     = " + invalidCount;
                    writeToLogFile(message);
                    System.out.println(message);
                }
                if (pdfCount!=0)
                {
                    message = "      Invoice files     = " + pdfCount;
                    writeToLogFile(message);
                    System.out.println(message);
                    message = "         Loaded = " + invoiceSuccessCount;
                    writeToLogFile(message);
                    System.out.println(message);
                    message = "         Failed = " + invoiceFailCount;
                    writeToLogFile(message);
                    System.out.println(message);
                }
                if (dailyCDRCount!=0)
                {
                    message = "      Daily CDR files   = " + dailyCDRCount;
                    writeToLogFile(message);
                    System.out.println(message);
                    message = "         Loaded = " + dailyCDRSuccessCount;
                    writeToLogFile(message);
                    System.out.println(message);
                    message = "         Failed = " + dailyCDRFailCount;
                    writeToLogFile(message);
                    System.out.println(message);
                }
                if (monthlyCDRCount!=0)
                {
                    message = "      Monthly CDR files = " + monthlyCDRCount;
                    writeToLogFile(message);
                    System.out.println(message);
                    message = "         Loaded = " + monthlyCDRSuccessCount;
                    writeToLogFile(message);
                    System.out.println(message);
                    message = "         Failed = " + monthlyCDRFailCount;
                    writeToLogFile(message);
                    System.out.println(message);
                }
            }                
            blankLine();
        }        
        // end of processing
        message = "Storm file processing completed";
        if (logging)
        {
            writeToLogFile(message);            
            closeLogFile();
        }
        System.out.println(message);
    }
    
    // process a daily CDR file
    private boolean processDailyCDRFile(String filename, String filedate, String accountnumber)
    {
        boolean result = false, OK = true;
        long accountId = 0;
        // check that the account exists and get its id
        if (createOracleObject())
        {
            accountId = oDB.getStormAccountId(accountnumber);
            if (accountId<=0)
            {               
                message = 
                    "   DBERROR: Failed to get account id (error=" + 
                    accountId +
                    ") for daily CDR file '" +
                    filename +
                    "'";
                writeToLogFile(message);
                System.out.println(message);
                moveToReject(filename);
                OK = false; 
            }
            destroyOracleObject();
        }
        if (OK)
        {
            // determine location of attachment 
            String attachmentDirPath = 
                attachDir + File.separator +
                STORM + File.separator +
                accountnumber;
            // create attachment directory if it does not already exist
            File attachmentDir = new File(attachmentDirPath);        
            if (!attachmentDir.exists())
                if (!attachmentDir.mkdirs())
                {
                    message = 
                        "   IOERROR: Failed to create attachment directory " + 
                        attachmentDirPath +
                        " for daily CDR file '" +
                        filename +
                        "'";
                    writeToLogFile(message);
                    System.out.println(message);
                    OK = false;
                    moveToReject(filename);
                }
        }
        if (OK)
        {
            // move daily CDR file to the attachment directory
            String oldPath = dropDir + File.separator + filename;
            File oldLocation = new File(oldPath);
            String newPath =                     
                attachDir + File.separator +
                STORM + File.separator +
                accountnumber + File.separator +
                filename;
            File newLocation = new File(newPath);
            if (newLocation.exists())
                if (!newLocation.delete())
                {
                   message = 
                        "   IOERROR: Failed to delete existing attachment '" + 
                        newPath +
                        "'";
                    writeToLogFile(message);
                    System.out.println(message);
                    OK = false;
                    moveToReject(filename);
                }
            if (OK)
            {
                if(!oldLocation.renameTo(newLocation))
                {                    
                   message = 
                        "   IOERROR: Failed to create new attachment '" + 
                        newPath +
                        "'";
                    writeToLogFile(message);
                    System.out.println(message);
                    OK = false;                    
                    moveToReject(filename);
                }
            }
        }
        if (OK)
        {
            // create attachment record on the database
            if (createOracleObject())
            {
                String location = 
                    STORM + FWDSLASH +
                    accountnumber + FWDSLASH +
                    filename;
                if (oDB.createAttachment(
                        location, 
                        DAILYCDR, 
                        accountId, 
                        ACCOUNT,
                        filedate,
                        YYYYMMDD,
                        CDR)!=1)
                {                                      
                   message = 
                        "   DBERROR: Failed to create new attachment '" + 
                        location +
                        "'";
                    writeToLogFile(message);
                    System.out.println(message);
                    OK = false;
                }
                //
                destroyOracleObject();
            }
        }
        if (OK)
            result = true;
        return result;
    }
    
    // process a monthly CDR file
    private boolean processMonthlyCDRFile(String filename, String filedate, String accountnumber, String invoicenumber)
    {
        boolean result = false, OK = true;
        long accountId = 0, invoiceId =0;
        String invoicenumberDB = "";
        // check that the account exists and get its id
        if (createOracleObject())
        {
            accountId = oDB.getStormAccountId(accountnumber);
            if (accountId<=0)
            {               
                message = 
                    "   DBERROR: Failed to get account id (error=" + 
                    accountId +
                    ") for monthly invoice PDF '" +
                    filename +
                    "'";
                writeToLogFile(message);
                System.out.println(message);
                OK = false;                 
                moveToReject(filename);
            }
            destroyOracleObject();
        }
        if (OK)
        {
            // Get invoice id
            if (createOracleObject())
            {
                invoiceId = oDB.findStormInvoice(accountId, invoicenumber, filedate);
                if (invoiceId<=0)
                {               
                    message = 
                        "   DBERROR: Failed to get invoice id (error=" + 
                        invoiceId +
                        ") for monthly invoice CDR file '" +
                        filename +
                        "'";
                    writeToLogFile(message);
                    System.out.println(message);
                    OK = false;                     
                    moveToReject(filename);
                }
                //System.out.println(invoiceId);
                destroyOracleObject();
            }
        }
        if (OK)
        {
            // get full invoice no            
            if (createOracleObject())
            {
                invoicenumberDB = oDB.getInvoiceNo(invoiceId);
                if ((invoicenumberDB.equals(UNKNOWN))||(invoicenumberDB.equals(NOVALUE)))
                {               
                    message = 
                        "   DBERROR: Failed to get invoice no for invoice id=" + 
                        invoiceId +
                        "(" +
                        invoicenumberDB +
                        ")";
                    writeToLogFile(message);
                    System.out.println(message);
                    OK = false; 
                    moveToReject(filename);
                }
                //System.out.println(invoicenumberDB);
                destroyOracleObject();
            }
        }       
        if (OK)
        {
            // determine location of attachment 
            String attachmentDirPath = 
                attachDir + File.separator +
                STORM + File.separator +
                accountnumber + File.separator +
                invoicenumberDB;
            // create attachment directory if it does not already exist
            File attachmentDir = new File(attachmentDirPath);        
            if (!attachmentDir.exists())
                if (!attachmentDir.mkdirs())
                {
                    message = 
                        "   IOERROR: Failed to create attachment directory " + 
                        attachmentDirPath +
                        " for monthly CDR file '" +
                        filename +
                        "'";
                    writeToLogFile(message);
                    System.out.println(message);
                    OK = false;
                    moveToReject(filename);
                }
        }         
        if (OK)
        {
            // move monthly CDR file to the attachment directory
            String oldPath = dropDir + File.separator + filename;
            File oldLocation = new File(oldPath);
            String newPath =                     
                attachDir + File.separator +
                STORM + File.separator +
                accountnumber + File.separator +
                invoicenumberDB + File.separator +
                filename;
            File newLocation = new File(newPath);
            if (newLocation.exists())
                if (!newLocation.delete())
                {
                   message = 
                        "   IOERROR: Failed to delete existing attachment '" + 
                        newPath +
                        "'";
                    writeToLogFile(message);
                    System.out.println(message);
                    OK = false;
                    moveToReject(filename);
                }
            if (OK)
            {
                if(!oldLocation.renameTo(newLocation))
                {                    
                   message = 
                        "   IOERROR: Failed to create new attachment '" + 
                        newPath +
                        "'";
                    writeToLogFile(message);
                    System.out.println(message);
                    OK = false;
                    moveToReject(filename);
                }
            }            
        } 
        if (OK)
        {
            // create attachment record on the database
            if (createOracleObject())
            {
                String location = 
                    STORM + FWDSLASH +
                    accountnumber + FWDSLASH +
                    invoicenumberDB + FWDSLASH +
                    filename;  
                int dateLength = filedate.length();
                String oracleDate =
                        "01-"+
                        filedate.substring(0, 3)+
                        "-"+
                        filedate.substring(dateLength - 2, dateLength);
                System.out.println(oracleDate);
                if (oDB.createAttachment(
                        location, 
                        MONTHLYCDR, 
                        invoiceId, 
                        INVOICE,
                        oracleDate,
                        DDMONYY,
                        CDR)!=1)
                {                                      
                   message = 
                        "   DBERROR: Failed to create new attachment '" + 
                        location +
                        "'";
                    writeToLogFile(message);
                    System.out.println(message);
                    OK = false;
                }
                //
                destroyOracleObject();
            }
        }
        if (OK)
            result = true;  
        return result;
    }    
    
    // process a monthly invoice file
    private boolean processInvoiceFile(String filename, String filedate, String accountnumber, String invoicenumber)
    {
        boolean result = false, OK = true;
        long accountId = 0, invoiceId =0;
        // check that the account exists and get its id
        if (createOracleObject())
        {
            accountId = oDB.getStormAccountId(accountnumber);
            if (accountId<=0)
            {               
                message = 
                    "   DBERROR: Failed to get account id (error=" + 
                    accountId +
                    ") for monthly invoice PDF '" +
                    filename +
                    "'";
                writeToLogFile(message);
                System.out.println(message);
                OK = false; 
                moveToReject(filename);
            }
            destroyOracleObject();
        }        
        if (OK)
        {
            // determine location of attachment 
            String attachmentDirPath = 
                attachDir + File.separator +
                STORM + File.separator +
                accountnumber + File.separator +
                invoicenumber;
            // create attachment directory if it does not already exist
            File attachmentDir = new File(attachmentDirPath);        
            if (!attachmentDir.exists())
                if (!attachmentDir.mkdirs())
                {
                    message = 
                        "   IOERROR: Failed to create attachment directory " + 
                        attachmentDirPath +
                        " for daily CDR file '" +
                        filename +
                        "'";
                    writeToLogFile(message);
                    System.out.println(message);
                    OK = false;
                    moveToReject(filename);
                }
        }        
        if (OK)
        {
            // move monthly PDF file to the attachment directory
            String oldPath = dropDir + File.separator + filename;
            File oldLocation = new File(oldPath);
            String newPath =                     
                attachDir + File.separator +
                STORM + File.separator +
                accountnumber + File.separator +
                invoicenumber + File.separator +
                filename;
            File newLocation = new File(newPath);
            if (newLocation.exists())
                if (!newLocation.delete())
                {
                   message = 
                        "   IOERROR: Failed to delete existing attachment '" + 
                        newPath +
                        "'";
                    writeToLogFile(message);
                    System.out.println(message);
                    OK = false;
                    moveToReject(filename);
                }
            if (OK)
            {
                if(!oldLocation.renameTo(newLocation))
                {                    
                   message = 
                        "   IOERROR: Failed to create new attachment '" + 
                        newPath +
                        "'";
                    writeToLogFile(message);
                    System.out.println(message);
                    OK = false;
                    moveToReject(filename);
                }
            }            
        }
        if (OK)
        {
            // Get invoice if creating it if it does not exist
            if (createOracleObject())
            {
                invoiceId = oDB.createStormInvoice(accountId, invoicenumber, filedate);
                if (invoiceId<=0)
                {               
                    message = 
                        "   DBERROR: Failed to get invoice id (error=" + 
                        invoiceId +
                        ") for monthly invoice PDF '" +
                        filename +
                        "'";
                    writeToLogFile(message);
                    System.out.println(message);
                    OK = false; 
                    moveToReject(filename);
                }
                //System.out.println(invoiceId);
                destroyOracleObject();
            }
        }
        if (OK)
        {
            // create attachment record on the database
            if (createOracleObject())
            {
                String location = 
                    STORM + FWDSLASH +
                    accountnumber + FWDSLASH +
                    invoicenumber + FWDSLASH +
                    filename;
                int dateLength = filedate.length();
                String workdate = 
                    "01" +
                    filedate.substring(0, 3) +
                    filedate.substring(dateLength - 2 , dateLength);                    
                if (oDB.createAttachment(
                        location, 
                        MONTHLYPDF, 
                        invoiceId, 
                        INVOICE,
                        workdate,
                        DDMONYY,
                        REPORT)!=1)
                {                                      
                   message = 
                        "   DBERROR: Failed to create new attachment '" + 
                        location +
                        "'";
                    writeToLogFile(message);
                    System.out.println(message);
                    OK = false;
                }
                //
                destroyOracleObject();
            }
        }
        if (OK)
            result = true;
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
            message = "DB ERROR: Failed to open ebilling database connection (1) : " + e.toString();
            System.out.println(message);
            writeToLogFile(message);
            result = false;
        } 
        return result;
    }
        
    // clears out OracleDB object
    private void destroyOracleObject()
    {
        try
        {
            conn.close();
            oDB = null;
        }
        catch(Exception e)
        {            
            message = "DB ERROR: Failed to close database connection : " + e.toString();
            System.out.println(message);
            writeToLogFile(message);
        }        
    }
    
    // move a file from the drop to the reject directory
    private void moveToReject(String filename)
    {
        File dropLocation = new File(dropDir+File.separator+filename);
        File rejectLocation = new File(rejDir+File.separator+filename);
        // check that file is not already in the reject directory
        if (rejectLocation.exists())
        {
            message = 
                "   IOERROR: Cannot move invalid file '" + 
                filename + 
                "' to reject directory as it is already there!";
            writeToLogFile(message);
            System.out.println(message);            
        }
        else
        {
            if (!dropLocation.renameTo(rejectLocation))
            {
                message = 
                    "   IOERROR: Failed to move invalid file '" + 
                    filename + 
                    "' to reject directory '" +
                    rejDir + 
                    "'";
                writeToLogFile(message);
                System.out.println(message);                
            }
        }
    }
    
    // returns separate parts of a filename delimted 
    // by underscores into a string array
    private String[] decodeFilename(String filename)
    {
        String results[] = new String[10];
        String testChar = "", part = "";
        int resultsPos = 1;
        // scan all but last four characters of the filename 
        // as that is the file extension
        for (int i = 0; i < filename.length() - 4; i++)
        {
            testChar = filename.substring(i,i+1);
            if (testChar.startsWith(USCORE))
            {
                results[resultsPos] = part;
                part = "";
                resultsPos++;
            }
            else
            {
                part = part + testChar;
            }
        }
        results[resultsPos] = part;
        return results;
    }
    
    // writes blank line to log and system output
    private void blankLine()
    {      
        message = "   ";
        System.out.println(message);
        writeToLogFile(message);   
    }
    
    // open log file
    private boolean openLogFile()
    {
        boolean result = true;
        logBW = null;
        String logDate = new java.util.Date().toString();
        String logFilename = 
            logDir+File.separator+"processStormFiles_"+
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
            result = false;
        }
        return result;
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

    
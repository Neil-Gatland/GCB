package batch;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

public class newEbillFeed 
{
    // oracle connection variables
    private String dbServer, service, username, password;
    private Connection conn, conn2;
    // OracleDB class variables
    private oracleDB oDB, oDB2;
    // file i/o variables
    private String logDir, dropDir, procDir, fileDir, attachDir; 
    private BufferedWriter logBW; 
    // logging variables
    private String message; 
    // String constants
    private final String USCORE = "_";
    private final String VPIPE = "|";
    private final String CFSTART = "Transfer_Control_File";
    private final String CONTROL = "Control";
    private final String REPORT = "Report";
    private final String YES = "Y";
    private final String GCB = "GCB";
    private final String FWDSLASH = "/";
    private final String REPORTTYPE = "report";
    private final String CDRTYPE = "cdr";
    private final String INVOICE = "invoice";
    // Various feed control statuses
    private final String IGNORED = "Ignored";
    private final String INSUSPENSE ="In Suspense";
    private final String PROCESSED ="Processed";
    // Various feed control / feed report processing message
    private final String IGNOREMESSAGE = 
            "Ignoring as the customer reference is flagged to use the old feed process";
    private final String MISSINGCRMESSAGE = 
            "Customer reference is missing";
    private final String FCSUCCESS = 
            "Invoice created or updated successfully";
    private final String MISSINGFILEMESSAGE = 
            "The invoice file is missing";
    private final String NOINVOICEMESSAGE = 
            "Invoice cannot be found";
    private final String NOATTACHDIRMESSAGE = 
            "Could not create all required attachment directories";
    private final String NOATTACHDELMESSAGE = 
            "Could not delete the existing attachment file for this invoice file";
    private final String NOATTACHCREATEMESSAGE = 
            "Failed to create the attachment file for the invoice file";
    private final String ATTACHFAILUREMESSAGE = 
            "Failed to create attachment DB record for the invoice file : ";
    private final String FRSUCCESS = 
            "Attachment created or updated successfully";
    // File extensions
    private final String LOGEXT = "_log.txt";
    //private final String PDFEXT = ".pdf";
    private final String ZIPEXT = ".zip";
    
    public static void main(String[] args)
    {
        // control processing
        newEbillFeed nEF = new newEbillFeed();
        nEF.control();
    }
    
    private void control()
    {
        System.out.println("newEbillFeed starting ...");
        // get relevant property values
        try
        {
          FileReader properties = new FileReader("feed.properties");
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
            if (propname.equals("procdir "))
                procDir = propval;
            if (propname.equals("filedir "))
                fileDir = propval;
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
          message = "Error accessing properties file --- " + e.toString();
          System.out.println(message);
        }
        openLogFile();
        writeToLogFile("New ebilling feed proccesing started");        
        // process all control files in the drop directory
        processControlFiles();
        // process loaded and in suspense feed control records
        processFeedControl();
        // process loaded and in suspense feed report records
        processFeedReport();
        writeToLogFile("New ebilling feed procesing completed");
        closeLogFile();
        System.out.println("newEbillFeed finshed ...");
    }   
    
    private void processFeedReport()
    {
        int osRowCount = 0, suspenseCount = 0, processedCount = 0;
        // process all Feed Report rows where the status is either 'Loaded' or 'In Suspense'
        if (createOracleObject())
        {
            ResultSet feedReportRS = oDB.getOutstandingFeedReport();
            try
            {
                while(feedReportRS.next())
                {
                    long feedReportId = feedReportRS.getLong("Feed_Report_Id");
                    String reportName = feedReportRS.getString("Report_Name");
                    String displayName = feedReportRS.getString("Display_Name");
                    String customerReference = feedReportRS.getString("Customer_Reference");
                    String accountNumber = feedReportRS.getString("Account_Number");
                    String invoiceNo = feedReportRS.getString("Invoice_No");
                    String taxPointDate = feedReportRS.getString("Tax_Point_Date");
                    long invoiceId = feedReportRS.getLong("Invoice_Id");
                    String fullAttachDirPath = "";
                    //System.out.println(feedReportId+":"+reportName+":"+displayName);
                    
                    // check if the report / extract exists
                    File invoiceReport = new File(fileDir+File.separator+reportName);
                    if (invoiceReport.exists())
                    {                        
                        boolean OK = true;
                        // check that the invoice exists
                        if (invoiceId==-1)
                        {
                           if (createOracleObject2())
                           {
                               if (oDB2.updateFeedReportStatus
                                       (feedReportId, INSUSPENSE, NOINVOICEMESSAGE)==1)
                                   suspenseCount++;
                               else
                               {                              
                                   message = 
                                       "   DB ERROR: Failed to set Feed Report (id="+
                                       feedReportId+") to "+
                                       INSUSPENSE;
                                   System.out.println(message);
                                   writeToLogFile(message);          
                               }
                           }
                           destroyOracleObject2();
                           OK = false; 
                        }
                        // move file to attachment directory location
                        if (OK)
                        {
                            fullAttachDirPath = 
                                attachDir + File.separator +
                                GCB + File.separator +
                                customerReference + File.separator +
                                accountNumber + File.separator +
                                invoiceNo;
                            File attachmentDir = new File(fullAttachDirPath);
                            // create all the required attachment directories if they don't exist
                            if (!attachmentDir.exists())
                                if (!attachmentDir.mkdirs())
                                {
                                    if (createOracleObject2())
                                    {
                                        if (oDB2.updateFeedReportStatus
                                                (feedReportId, INSUSPENSE, NOATTACHDIRMESSAGE)==1)
                                            suspenseCount++;
                                        else
                                        {                              
                                            message = 
                                                "   DB ERROR: Failed to set Feed Report (id="+
                                                feedReportId+") to "+
                                                INSUSPENSE;
                                            System.out.println(message);
                                            writeToLogFile(message);          
                                        }
                                    }
                                    destroyOracleObject2();
                                    OK = false;
                                }
                        }                        
                        if (OK)
                        {
                            // create attachment file
                            File attachmentFile = 
                                    new File(fullAttachDirPath+File.separator+reportName);
                            if (attachmentFile.exists())
                            {
                                // delete if it already exists
                                if (!attachmentFile.delete())
                                    {
                                        if (createOracleObject2())
                                        {
                                            if (oDB2.updateFeedReportStatus
                                                    (feedReportId, INSUSPENSE, NOATTACHDELMESSAGE)==1)
                                                suspenseCount++;
                                            else
                                            {                              
                                                message = 
                                                    "   DB ERROR: Failed to set Feed Report (id="+
                                                    feedReportId+") to "+
                                                    INSUSPENSE;
                                                System.out.println(message);
                                                writeToLogFile(message);          
                                            }
                                        }
                                        destroyOracleObject2();
                                        OK = false;                                    
                                }                               
                            }
                            if (OK)
                            {
                                // move file from outstanding directory
                                // to relevant attachment directory
                                if (!invoiceReport.renameTo(attachmentFile))
                                {
                                    if (createOracleObject2())
                                    {
                                        if (oDB2.updateFeedReportStatus
                                                (feedReportId, INSUSPENSE, NOATTACHCREATEMESSAGE)==1)
                                            suspenseCount++;
                                        else
                                        {                              
                                            message = 
                                                "   DB ERROR: Failed to set Feed Report (id="+
                                                feedReportId+") to "+
                                                INSUSPENSE;
                                            System.out.println(message);
                                            writeToLogFile(message);          
                                        }
                                    }
                                    destroyOracleObject2();
                                    OK = false;
                                }
                            }
                        }     
                        if (OK)
                        {                   
                            // create DB attachment record
                            String location =
                                GCB +FWDSLASH+
                                customerReference + FWDSLASH+
                                accountNumber+FWDSLASH +
                                invoiceNo+FWDSLASH+
                                reportName;
                            String itemType = REPORTTYPE;
                            if (reportName.endsWith(ZIPEXT))
                                itemType = CDRTYPE;
                            //System.out.println(location+":"+fileType);
                            long caResult = 0;
                            if (createOracleObject2())
                            {
                                caResult =
                                    oDB2.createAttachment(
                                        location, 
                                        displayName, 
                                        invoiceId, 
                                        INVOICE, 
                                        taxPointDate, 
                                        itemType);                            }
                                
                            destroyOracleObject2();
                            if (caResult!=1)
                            {
                                if (createOracleObject2())
                                {
                                    String processMessage = ATTACHFAILUREMESSAGE;
                                    if (caResult==-1)
                                        processMessage = processMessage + 
                                            "multiple existing attachments";
                                    else if (caResult==-2)
                                        processMessage = processMessage + 
                                            "fails updating existing attachment";
                                    else if (caResult==-3)
                                        processMessage = processMessage +
                                            "fails creating new attachment";
                                    else if (caResult==-999)
                                        processMessage = processMessage + 
                                            "unexpected error";
                                    else 
                                        processMessage = processMessage + 
                                            "untrapped error";                                        
                                    if (oDB2.updateFeedReportStatus
                                            (feedReportId, INSUSPENSE, processMessage)==1)
                                        suspenseCount++;
                                    else
                                    {                              
                                        message = 
                                            "   DB ERROR: Failed to set Feed Report (id="+
                                            feedReportId+") to "+
                                            INSUSPENSE;
                                        System.out.println(message);
                                        writeToLogFile(message);          
                                    }
                                }
                                destroyOracleObject2();
                                OK = false;
                            }
                        }
                        // update feed report status to processed
                        if (OK)
                        {
                            if (createOracleObject2())
                            {
                                if (oDB2.updateFeedReportStatus
                                        (feedReportId, PROCESSED, FRSUCCESS)==1)
                                    processedCount++;
                                else
                                {                              
                                    message = 
                                        "   DB ERROR: Failed to set Feed Report (id="+
                                        feedReportId+") to "+
                                        PROCESSED;
                                    System.out.println(message);
                                    writeToLogFile(message);          
                                }
                            }
                            destroyOracleObject2();
                        }
                    }
                    else
                    {
                        // invoice file is not in outstanding files directory
                        if (createOracleObject2())
                        {
                            if (oDB2.updateFeedReportStatus
                                    (feedReportId, INSUSPENSE, MISSINGFILEMESSAGE)==1)
                                suspenseCount++;
                            else
                            {                              
                                message = 
                                    "   DB ERROR: Failed to set Feed Report (id="+
                                    feedReportId+") to "+
                                    INSUSPENSE;
                                System.out.println(message);
                                writeToLogFile(message);          
                            }
                        }
                        destroyOracleObject2();
                    }
                    osRowCount++;
                }
            }
            catch(java.sql.SQLException ex)
            {
                message = 
                    "   DB ERROR: Failed to execute Get_Outstanding_Report Control "+
                    ex.getMessage();                
                //System.out.println(message);
                writeToLogFile(message);
            }
            feedReportRS = null;            
        }
        destroyOracleObject();  
        // summarise feed control processing
        message = "   Processing outstanding feed report";
        System.out.println(message);
        writeToLogFile(message); 
        message = "      Outstanding feed report      : "+osRowCount;
        System.out.println(message);
        writeToLogFile(message); 
        message = "      Suspended                    : "+suspenseCount;
        System.out.println(message);
        writeToLogFile(message);
        message = "      Processed                    : "+processedCount;
        System.out.println(message);
        writeToLogFile(message); 
        blankLine();
    }  
    
    private void processFeedControl()
    {
        int osRowCount = 0, ignoredCount = 0, suspenseCount = 0, processedCount = 0;
        // process all Feed Control rows where the status is either 'Loaded' or 'In Suspense'
        if (createOracleObject())
        {
            ResultSet feedControlRS = oDB.getOutstandingFeedControl();
            try
            {
                while (feedControlRS.next())
                {
                    long feedControlId = feedControlRS.getLong("Feed_Control_Id");
                    String customerReference = feedControlRS.getString("Customer_Reference");
                    String invoiceNo = feedControlRS.getString("Invoice_No");
                    long customerId = feedControlRS.getLong("Customer_Id");
                    String useOldFeed = feedControlRS.getString("Use_Old_Feed");
                    //System.out.println(customerReference+":"+invoiceNo+":"+customerId+":"+useOldFeed);
                    
                    // Ignore any rows where Use Old Feed is set to 'Y'                    
                    if (useOldFeed.startsWith(YES))
                    {
                        if (createOracleObject2())
                        {
                            if (oDB2.updateFeedControlStatus
                                    (feedControlId, IGNORED, IGNOREMESSAGE)==1)
                                ignoredCount++;
                            else
                            {                              
                                message = 
                                    "   DB ERROR: Failed to set Feed Control (id="+
                                    feedControlId+") to "+
                                    IGNORED;
                                System.out.println(message);
                                writeToLogFile(message);          
                            }
                        }
                        destroyOracleObject2();                        
                    }
                    // Suspend any rows where the Customer Reference is missing
                    // (where Customer Id is -999
                    else if (customerId==-999)
                    {                        
                        if (createOracleObject2())
                        {
                            if (oDB2.updateFeedControlStatus
                                    (feedControlId, INSUSPENSE, MISSINGCRMESSAGE)==1)
                                suspenseCount++;
                            else
                            {                              
                                message = 
                                    "   DB ERROR: Failed to set Feed Control (id="+
                                    feedControlId+") to "+
                                    MISSINGCRMESSAGE;
                                System.out.println(message);
                                writeToLogFile(message);          
                            }
                        }
                        destroyOracleObject2();                        
                    }
                    // Process all other rows
                    else
                    {
                        // Check if the account exists and if not whether it can be set up
                        String accountNumber = feedControlRS.getString("Account_Number");
                        String accountName = feedControlRS.getString("Account_Name");
                        String billingSource = feedControlRS.getString("Billing_Source");
                        //System.out.println(accountNumber+":"+accountName+":"+billingSource); 
                        long accountId  = -777;
                        if (createOracleObject2())
                        {
                            accountId = 
                                oDB2.getAccountId(
                                    customerId, 
                                    customerReference, 
                                    billingSource, 
                                    accountNumber, 
                                    accountName);
                        }
                        destroyOracleObject2(); 
                        //System.out.println(accountId);
                        if (accountId>0)
                        {
                            // set up or amend invoice
                            String taxPointDate = feedControlRS.getString("Tax_Point_Date");
                            String periodFromDate = feedControlRS.getString("Period_From_Date");
                            String periodToDate = feedControlRS.getString("Period_To_Date");
                            String invoiceTotal = feedControlRS.getString("Invoice_Total");
                            String currency = feedControlRS.getString("ISO_Currency_Code");
                            //System.out.println(taxPointDate+":"+periodFromDate+":"+periodToDate+":"+invoiceTotal);
                            long invoiceId = -777;
                            if (createOracleObject2())
                            {
                                invoiceId = 
                                    oDB2.createInvoice(
                                        accountId, 
                                        invoiceNo, 
                                        currency, 
                                        taxPointDate, 
                                        periodFromDate, 
                                        periodToDate, 
                                        invoiceTotal);
                            }
                            destroyOracleObject2();
                            //System.out.println(invoiceId);if (createOracleObject2()) 
                            if (invoiceId > 0)
                            { 
                                // set feed report row to processed
                                if (createOracleObject2())
                                {
                                    if (oDB2.updateFeedControlStatus
                                            (feedControlId, PROCESSED, FCSUCCESS)==1)                                    
                                        processedCount++;
                                    else
                                    {                              
                                        message = 
                                            "   DB ERROR: Failed to set Feed Control (id="+
                                                feedControlId+") to "+
                                                PROCESSED;
                                        System.out.println(message);
                                        writeToLogFile(message);          
                                    }   
                                }
                                destroyOracleObject2();                                 
                            }
                            else
                            {
                                // invoice set up / amendment has failed
                                String procMess = "Error create invoice : ";
                                if (accountId==-1)
                                    procMess = procMess + 
                                        "existing invoice duplicated";
                                else if (accountId==-2)
                                    procMess = procMess + 
                                        "error checking for invoice";
                                else if (accountId==-3)
                                    procMess = procMess + 
                                        "invoice exists for a different account";
                                else if (accountId==-4)
                                    procMess = procMess + 
                                        "error creating new invoice";
                                else if (accountId==-5)
                                    procMess = procMess + 
                                        "error updating invoice";
                                else if (accountId==-999)
                                    procMess = procMess + 
                                        "unexpected error";
                                if (createOracleObject2())
                                { 
                                    if (oDB2.updateFeedControlStatus
                                            (feedControlId, INSUSPENSE, procMess)==1)
                                        suspenseCount++;
                                    else
                                    {                              
                                    message = 
                                        "   DB ERROR: Failed to set Feed Control (id="+
                                        feedControlId+") to "+
                                        INSUSPENSE;
                                    System.out.println(message);
                                    writeToLogFile(message);          
                                    }                                    
                                }
                                destroyOracleObject2();
                            }
                        }
                        else
                        {
                            // set to In Suspense with appropriate message 
                            // as unable to get account id
                            String procMess = "Error getting account id : ";
                            if (accountId==-1)
                                procMess = procMess + 
                                    "updating account name";
                            else if (accountId==-2)
                                procMess = procMess + 
                                    "more than one 'standard' payment group and no default payment group";
                            else if (accountId==-3)
                                procMess = procMess + 
                                    "finding 'standard' payment group";
                            else if (accountId==-4)
                                procMess = procMess + 
                                    "creating 'standard' payment group";
                            else if (accountId==-5)
                                procMess = procMess + 
                                    "creating new account";
                            else if (accountId==-999)
                                procMess = procMess + 
                                    "unexpected error";
                            if (createOracleObject2())
                            {
                                if (oDB2.updateFeedControlStatus
                                        (feedControlId, INSUSPENSE, procMess)==1)
                                    suspenseCount++;
                                else
                                {                              
                                    message = 
                                        "   DB ERROR: Failed to set Feed Control (id="+
                                        feedControlId+") to "+
                                        INSUSPENSE;
                                    System.out.println(message);
                                    writeToLogFile(message);          
                                }
                            }
                            destroyOracleObject2(); 
                            }
                    }
                    osRowCount++;
                }
            }
            catch(java.sql.SQLException ex)
            {
                message = 
                    "   DB ERROR: Failed to execute Get_Outstanding_Feed Control "+
                    ex.getMessage();                
                System.out.println(message);
                writeToLogFile(message);
            }
            feedControlRS = null;
        }
        destroyOracleObject();        
        // summarise feed control processing
        message = "   Processing outstanding feed control";
        System.out.println(message);
        writeToLogFile(message); 
        message = "      Outstanding feed control     : "+osRowCount;
        System.out.println(message);
        writeToLogFile(message); 
        message = "      Ignored (use old feed)       : "+ignoredCount;
        System.out.println(message);
        writeToLogFile(message); 
        message = "      Suspended                    : "+suspenseCount;
        System.out.println(message);
        writeToLogFile(message);
        message = "      Processed                    : "+processedCount;
        System.out.println(message);
        writeToLogFile(message);
        blankLine();
    }
    
    private void processControlFiles()
    {
        // blank log message line
        blankLine();
        // check for control files in the drop directory
        int controlFileCount = 0, processedControlFileCount=0;
        int controlCount = 0, processedControlCount = 0;
        int reportCount =0, processedReportCount = 0, movedReportCount = 0;
        long feedControlId = 0, feedReportId = 0;
        String cfLine = "";
        File dropDirectory = new File(dropDir);
        FilenameFilter controlFilter = new FilenameFilter()
        {
          public boolean accept(File dir, String name)
          {
            return name.startsWith(CFSTART);
          }
        };
        File[] controlFileArray = dropDirectory.listFiles(controlFilter);
        // process each control file in the drop directory
        for (int i = 0; i < controlFileArray.length; i++)
        {
            String controlFilename = controlFileArray[i].getName();
            controlFileCount++;
            Boolean ok = true;
            // Check that the control file is not a duplicate
            File processedControlFile = new File(procDir+File.separator+controlFilename);
            if (processedControlFile.exists())
            {
                message = 
                    "I/O ERROR: Control file "+
                    controlFilename + 
                    " has already been processed";
                System.out.println(message);
                writeToLogFile(message);
                ok = false;
            }
            if (ok)
            {                   
                // open the control file for reading
                BufferedReader cfBR = openBufferedFile(dropDir,controlFilename);
                if (cfBR==null)
                {
                    // unable to open the control file
                    message = "I/O ERROR: Failed to open control file "+controlFilename; 
                    System.out.println(message);
                    writeToLogFile(message);
                    ok = false;
                } 
                if (ok)
                {                  
                    // read first line of control line
                    cfLine = getBufferedFileLine(cfBR);
                    if (cfLine==null)
                    {
                        // the control file is empty
                        message = "I/O ERROR: Control file "+controlFilename+" is empty"; 
                        System.out.println(message);
                        writeToLogFile(message);
                        ok = false;
                    }  
                }
                if (ok)
                {
                    // read each line of the control file
                    while(cfLine!=null)
                    {
                        // decode line
                        String[] lineItems = decodeLine(cfLine);
                        String lineType = lineItems[0];
                        // 
                        if (lineType.startsWith(CONTROL))
                        {
                            controlCount++;
                            String customerReference = lineItems[1];
                            String invoiceNo = lineItems[2];
                            String accountNumber = lineItems[3];
                            String accountName = lineItems[4];
                            String billingSource = lineItems[5];
                            String currency = lineItems[6];
                            String taxPointDate = lineItems[7];
                            String periodFromDate = lineItems[8];
                            String periodToDate = lineItems[9];
                            String invoiceTotal = lineItems[10];
                            /*System.out.println("CONTROL: "+
                                    customerReference+":"+
                                    invoiceNo+":"+
                                    accountNumber+":"+
                                    accountName+":"+
                                    billingSource+":"+
                                    currency+":"+
                                    taxPointDate+":"+
                                    periodFromDate+":"+
                                    periodToDate+":"+
                                    invoiceTotal); */  
                            if (createOracleObject())       
                               feedControlId = 
                                    oDB.insertFeedControl(
                                        billingSource, 
                                        customerReference, 
                                        invoiceNo, 
                                        accountNumber, 
                                        accountName, 
                                        currency, 
                                        taxPointDate, 
                                        periodFromDate, 
                                        periodToDate, 
                                        invoiceTotal, 
                                        controlFilename) ;
                            if (feedControlId<1)
                            {
                                message = 
                                    "   DB ERROR: Failed to insert Feed_Control for "+
                                    customerReference+VPIPE+
                                    invoiceNo;                           
                                System.out.println(message);
                                writeToLogFile(message);
                            }
                            else
                                processedControlCount++;
                            destroyOracleObject();
                        }
                        else if (lineType.startsWith(REPORT))
                        {
                            reportCount++;
                            String reportFilename = lineItems[1];
                            String displayName = lineItems[2];
                            //System.out.println("REPORT: "+reportFilename+":"+displayName);
                            if (createOracleObject()) 
                                feedReportId = 
                                    oDB.insertFeedReport(
                                        feedControlId, 
                                        reportFilename,
                                        displayName); 
                            if (feedReportId<1)
                            {
                                message = 
                                    "   DB ERROR: Failed to insert Feed_Report for "+
                                        reportFilename;                           
                                System.out.println(message);
                                writeToLogFile(message);
                            }
                            else
                            {
                                processedReportCount++;
                                // move report file to file directory
                                File dropReportFile = 
                                    new File(dropDir+File.separator+reportFilename);
                                File movedReportFile = 
                                    new File(fileDir+File.separator+reportFilename);
                                if (dropReportFile.exists())
                                {                                   
                                    if (movedReportFile.exists())
                                    {
                                        message = 
                                            "   I/O ERROR: Cannot move report file "+
                                                reportFilename+
                                                " as it is already in file directory";
                                        System.out.println(message);
                                        writeToLogFile(message);
                                    }
                                    else
                                    {
                                        if (dropReportFile.renameTo(movedReportFile))
                                            movedReportCount++;
                                        else
                                        {
                                            message = 
                                                "   I/O ERROR: Failed to move report file "+
                                                reportFilename;
                                            System.out.println(message);
                                            writeToLogFile(message);
                                        }
                                    } 
                                }
                                else
                                {
                                    message = 
                                        "   I/O ERROR: Cannot move missing report file "+
                                            reportFilename;
                                    System.out.println(message);
                                    writeToLogFile(message);
                                    
                                }
                            }                                
                            destroyOracleObject();
                        }
                        else
                        {
                            message = 
                                "Invalid record type of "+
                                lineType+
                                " in control file "+
                                controlFilename;                            
                            System.out.println(message);
                            writeToLogFile(message);
                        }
                        // read next line
                        cfLine = getBufferedFileLine(cfBR); 
                    }
                }
                if (ok)
                {
                    // close the control file
                    if (closeBufferedFile(cfBR))
                    {
                        message = "   Processed control file "+controlFilename;   
                        System.out.println(message);
                        writeToLogFile(message);
                        processedControlFileCount++; 
                    }
                    else
                    {
                        // failed to close control file
                        message = "I/O ERROR: Failed to close control file "+controlFilename; 
                        System.out.println(message);
                        writeToLogFile(message);
                        ok = false;
                    }
                } 
                if (ok)
                {
                    // move the control file
                    if (!controlFileArray[i].renameTo(processedControlFile))
                    {
                        // failed to move control file
                        message = "I/O ERROR: Failed to move control file "+controlFilename; 
                        System.out.println(message);
                        writeToLogFile(message);
                        ok = false;                        
                    }
                }
            }
        }
        if (controlFileCount==0)
        {
            // no control files
            message = "   No control files in the drop directory to process";
            System.out.println(message);
            writeToLogFile(message); 
        }
        else
        {
            // summarise control file processing
            blankLine();
            message = "   Control files in drop directory : "+controlFileCount;
            System.out.println(message);
            writeToLogFile(message); 
            message = "   Control files processed         : "+processedControlFileCount;
            System.out.println(message);
            writeToLogFile(message); 
            message = "      Control records identified   : "+controlCount;
            System.out.println(message);
            writeToLogFile(message); 
            message = "      Control records added to DB  : "+processedControlCount;
            System.out.println(message);
            writeToLogFile(message);
            message = "      Report records identified    : "+reportCount;
            System.out.println(message);
            writeToLogFile(message); 
            message = "      Report records added to DB   : "+processedReportCount;
            System.out.println(message);
            writeToLogFile(message); 
            message = "      Report files moved           : "+movedReportCount;
            System.out.println(message);
            writeToLogFile(message); 
        }
        blankLine();
    }
    
    // writes blank line to log and system out
    private void blankLine()
    {      
        message = "   ";
        System.out.println(message);
        writeToLogFile(message);   
    }
        
    // open log file
    private void openLogFile()
    {
        logBW = null;
        String logDate = new java.util.Date().toString();
        String logFilename = 
            logDir+File.separator+"newEbillingFeed_"+
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
    
    // open a BuffferedReader for a file
    private BufferedReader openBufferedFile(String directory, String filename)
    {
        // open file for reading
        BufferedReader readFile = null;
        try
        {
          readFile = new BufferedReader(new FileReader(directory+File.separator+filename));
        }
        catch (Exception ex)
        {
          System.out.println("Failed to open file "+directory+File.separator+filename+" : "+ex.getMessage());
        }
        return readFile;
    }

    // get next line from a BufferedReader
    private String getBufferedFileLine(BufferedReader readFile)
    {
        String fileLine = "";
        try
        {
          fileLine = readFile.readLine();
        }
        catch (Exception ex)
        {
        System.out.println("Failed to read BufferedReader : "+ex.getMessage());
        }
        return fileLine;
    }

    // close a BufferedReader
    private boolean closeBufferedFile(BufferedReader readFile)
    {
        // close file
        boolean result = false;
        try
        {
            readFile.close();
            result = true;
        }
        catch (Exception ex)
        {
        System.out.println("Failed to close BufferedReader : "+ex.getMessage());
        }
        return result;
    }
    
    // decode control file lines where data is separated by vertical pipes
    private String[] decodeLine(String fileLine)
    {
        String [] lineData = new String[30];
        String lineItem = "";
        int itemPos = 0;
        for (int i=0; i<fileLine.length(); i++)
        {
            String testChar = fileLine.substring(i,i+1);
            if (testChar.equals(VPIPE))
            {
                // item complete
                lineData[itemPos] = lineItem;
                lineItem = "";
                itemPos++;
            }
            else
                // add character to current item
                lineItem = lineItem + testChar;
        }
        // last item (which has no vertical pipe following)
        lineData[itemPos] = lineItem;
        return lineData;
    }
    
    // creates 'outer' OracleDB object (used for looping)
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
    
    // create 'inner' OracleDB object (uses for updates, inerts, etc)
    private boolean createOracleObject2()
    {
        boolean result = false;
        // open database connection
        String url = "jdbc:oracle:thin:@"+dbServer+":1521:"+service;
        try
        {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            conn2 = DriverManager.getConnection(url,username,password);
            oDB2 = new oracleDB(conn2);
            result = true;
        }
        catch(Exception e)
        {            
            message = "DB ERROR: Failed to open ebilling database connection (2) : " + e.toString();
            System.out.println(message);
            writeToLogFile(message);
            result = false;
        } 
        return result;
    }
    
    // clears out 'outer' OracleDB object
    private void destroyOracleObject()
    {
        boolean result = false;
        try
        {
            conn.close();
            oDB = null;
        }
        catch(Exception e)
        {            
            message = "DB ERROR: Failed to close database connection (1) : " + e.toString();
            System.out.println(message);
            writeToLogFile(message);
        }        
    }
    
    // clears down 'inner' OracleDB object
    private void destroyOracleObject2()
    {
        boolean result = false;
        try
        {
            conn2.close();         
            oDB2 = null;
        }
        catch(Exception e)
        {            
            message = "DB ERROR: Failed to close database connection (2) : " + e.toString();
            System.out.println(message);
            writeToLogFile(message);
        }        
    } 
            
}

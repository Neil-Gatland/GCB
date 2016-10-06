package batch;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

public class attachmentExtract 
{
    
    // oracle connection variables
    private String dbServer, service, username, password;
    private Connection conn;
    // OracleDB class variables
    private oracleDB oDB;
    // file i/o variables
    private String logDir, outDir, attachDir, archiveDir;    
    private BufferedWriter logBW, controlFileBW, errorFileBW;
    // run parameter variables
    private String billingSource, startDate, endDate, bSource;
    // logging variables
    private String message; 
    // String constants    
    private final String USCORE = "_";
    private final String FWDSLASH = "/";
    private final String COMMA = ",";
    private final String LOG = "log";
    private final String CONTROL = "control";
    private final String ERROR = "error";
    private final String REPORT = "report";
    private final String MONTHLYCDR = "monthlycdr";
    private final String DAILYCDR = "dailycdr";
    private final String ATTACHMENT = "attachment";
    private final String INVOICE = "Invoice";
    private final String INVOICEDETAIL = "Invoice Detail";
    private final String OTHERREPORTS = "Other Report(s)";
    private final String DETAIL = "Detail";
    private final String SERVICESUMMARY = "Service Summary";
    private final String SERVICEREFERENCEDETAIL = "Service Reference Detail";
    private final String INVOICEQUARTERLY = "Invoice (Quarterly)";
    private final String ARBORMONTHLYINVOICEDCDRS = "Arbor Monthly Invoiced CDRs";
    private final String MONTHLYCDRS = "Monthly CDRs";
    private final String ARBORMONTHLYINVOICEDCDRSPART = "Arbor Monthly Invoiced CDRs Part";
    private final String MONTHLYCDRSPART = "Monthly CDRs Part";
    private final String ARBORMONTHLYCDRSUMMARIES = "Arbor Monthly Invoiced CDR Summaries";
    private final String MONTHLYCDRSUMMARIES = "Monthly CDR Summaries";
    private final String IPTRUNKINGCDRS = "IP Trunking cdrs";
    private final String MONTHLYIPTRUNKINGCDRS = "Monthly IP Trunking CDRs";
    private final String DAILYCDRS = "Daily CDRs";
    private final String MONTHLYCDRSROI = "Monthly CDRS";
    private final String MONTHLYWLRCDRSROI = "Monthly WLR CDRS";
    private final String MONTHLYWLRCDRS = "Monthly WLR CDRs";
    private final String INVOICEBACKUP = "Invoice Backup";
    private final String SSBSMONTHLYINVOICEDPSTNCDRS = "SSBS Monthly Invoiced PSTN CDRs";
    private final String SSBSMONTHLYINVOICEDPSTNCDRSPART = "SSBS Monthly Invoiced PSTN CDRs Part";
    private final String SSBSMONTHLYINVOICEDPSTNSUMMARIES = "SSBS Monthly Invoiced PSTN Summaries";
    private final String SSBSMONTHLYINVOICEDTNBSCDRS = "SSBS Monthly Invoiced TNBS CDRs";
    private final String SSBSMONTHLYINVOICEDTNBSCDRSPART = "SSBS Monthly Invoiced TNBS CDRs Part";
    private final String SSBSMONTHLYINVOICEDTNBSSUMMARIES = "SSBS Monthly Invoiced TNBS Summaries";
    private final String SSBSENDOFMONTHTNBSCDRS = "SSBS End of Month TNBS CDRs";
    private final String SSBSENDOFMONTHTNBSCDRSPART = "SSBS End of Month TNBS CDRs Part";
    private final String SSBSENDOFMONTHTNBSSUMMARIES = "SSBS End of Month TNBS Summaries";
    private final String MONTHLYPSTNCDRS = "Monthly PSTN CDRs";
    private final String MONTHLYPSTNCDRSPART = "Monthly PSTN CDRs Part";
    private final String MONTHLYPSTNCDRSUMMARIES = "Monthly PSTN CDR Summaries";
    private final String MONTHLYTNBSCDRS = "Monthly TNBS CDRs";
    private final String MONTHLYTNBSCDRSPART = "Monthly TNBS CDRs Part";
    private final String MONTHLYTNBSCDRSUMMARIES = "Monthly TNBS CDR Summaries";
    private final String ENDOFMONTHTNBSCDRS = "End of Month TNBS CDRs";
    private final String ENDOFMONTHTNBSCDRSPART = "End of Month TNBS CDRs Part";
    private final String ENDOFMONTHTNBSCDRSUMMARIES = "End of Month TNBS CDR Summaries";
    private final String SSBSDAILYPSTNCDRS = "SSBS Daily PSTN CDRs";
    private final String SSBSDAILYTNBSCDRS = "SSBS Daily TNBS CDRs";
    private final String DAILYPSTNCDRS = "Daily PSTN CDRs";
    private final String DAILYTNBSCDRS = "Daily TNBS CDRs";
    private final String SSBSALARMCALL = "SSBS Alarm-call";
    private final String ALARMCALLS = "Alarm Calls";
    private final String STORMMONTHLYCDRS = "Storm Monthly CDRs";
    private final String STORMDAILYCDRS = "Storm Daily CDRs";
    private final String CONTROLHEADER = 
        "Billing Source,AML Billing Source,Account Number,Account Name,"+
        "Invoice No,Tax Point Date,Period From Date,Period To Date,"+
        "Invoice Total,Currency Code,Attachment_Date,File Type,File Name";
    // File extensions
    private final String CSVTEXT = ".csv";
    private final String TEXTEXT = ".txt";
    private final String PDFEXT = ".pdf";
    // ebilling Billing Sources
    private final String ARBOR = "Arbor";
    private final String BASA = "BASA";
    private final String ROI = "ebillz";
    private final String ROIOUT = "ROI";
    private final String GCB1 = "GCB Data";
    private final String GCB3 = "Conglom";
    private final String GCB5 = "MS";
    private final String GCB7 = "Globaldial";
    private final String GCB1OUT = "GCB1";
    private final String GCB3OUT = "GCB3";
    private final String GCB5OUT = "GCB5";
    private final String GCB7OUT = "GCB7";
    private final String IVIW = "IVIW";
    private final String MOBI = "MOBI";
    private final String PCB = "PCB";
    private final String QB = "QB";
    private final String RSMA = "RevShare";
    private final String RSMAOUT = "RSMA";
    private final String SSBS = "SSBS";
    private final String STORM = "Storm";
    private final String VBS = "VBS";
    // AML Billing Source
    private final String PESP = "PESP";
    private final String GCBR = "GCBR";
    private final String DNS = "DNS";
    private final String STRM = "STRM";
    private final String EBLZ = "EBLZ";
    private final String MOVE = "MOVE";
    // Non-Unique Arbor File names
    private final String ARBORSTDINVOICE = "Arbor_Invoice_S.pdf";
    private final String ARBORSTDDETAIL = "Arbor_Invoice_D.pdf";
    private final String ARBORSTDCREDIT = "Arbor_Credit_S.pdf";
    // Duplicate text suffix
    private final String DUPTEXT = "DUP";
        
    public static void main(String[] args)
    {
        // control processing
        attachmentExtract aE = new attachmentExtract();
        aE.control();
    }
    
    private void control()
    {
        // get relevant property values
        try
        {
          FileReader properties = new FileReader("AttachmentExtract.properties");   
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
            if (propname.equals("logDir  "))
                logDir = propval;   
            if (propname.equals("billSrc "))
                billingSource = propval; 
            if (propname.equals("strtDate"))
                startDate = propval;    
            if (propname.equals("endDate "))
                endDate = propval;      
            if (propname.equals("outDir  "))
                outDir = propval;      
            if (propname.equals("attach  "))
                attachDir = propval;      
            if (propname.equals("archive "))
                archiveDir = propval;           
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
        outputMessage("Attachment extract proccesing started at "+time);
        boolean validParameters = true;
        // Validate supplied billing source, start date and end date
        if (!validBillingSource(billingSource))
        {
            validParameters = false;
            message = "   Invalid supplied billing source " + billingSource;
            outputMessage(message);
        }
        // Set up output billing source names for GCB1/3/5/7
        bSource = billingSource;
        if (billingSource.startsWith(GCB1))
                bSource = GCB1OUT;
        if (billingSource.startsWith(GCB3))
                bSource = GCB3OUT;
        if (billingSource.startsWith(GCB5))
                bSource = GCB5OUT;
        if (billingSource.startsWith(GCB7))
                bSource = GCB7OUT;
        if (billingSource.startsWith(RSMA))
                bSource = RSMAOUT;
        if (billingSource.startsWith(ROI))
                bSource = ROIOUT;
        // Validate supplied start and end dates
        if (createOracleObject())
        {
            String validationResult = oDB.validateDateRange(startDate, endDate);
            if (!validationResult.equals("Date range is valid"))
            {            
                validParameters = false;
                message = "   " + validationResult;
                outputMessage(message);  
            }
        }
        else
            // stop processing if not connected to Oracle
            validParameters = false;
        destroyOracleObject();
        if (validParameters) 
            extractRelevantAttachments();
        time = new java.util.Date().toString().substring(11,19);
        outputMessage("Attachment extract procesing completed at "+time);
        closeLogFile();
    } 
    
    private void extractRelevantAttachments()
    {  
        // open control and error files
        openControlFile(); 
        openErrorFile();
        int controlCount = 0, errorCount = 0,
            reportCount = 0, monthlyCDRCount = 0, 
            dailyCDRCount = 0, attachmentCount = 0,
            reportErrorCount = 0, monthlyCDRErrorCount = 0, 
            dailyCDRErrorCount = 0, attachmentErrorCount = 0;
        
        // write control file header
        writeToControlFile(CONTROLHEADER);
        
        // process invoice reports
        if (createOracleObject())
        {            
            ResultSet invoiceReportRS = oDB.getInvoiceReports(billingSource, startDate, endDate);
            try
            {
                while(invoiceReportRS.next())
                {
                    // Store invoice report values
                    String accountNumber = invoiceReportRS.getString("Account_Number");
                    String accountName = invoiceReportRS.getString("Account_Name");
                    String invoiceNo = invoiceReportRS.getString("Invoice_No");
                    String taxPointDate = invoiceReportRS.getString("Tax_Point_Date");
                    String periodFromDate = invoiceReportRS.getString("Period_From_Date");
                    String periodToDate = invoiceReportRS.getString("Period_To_Date");
                    String invoiceTotal = invoiceReportRS.getString("Invoice_Total");
                    String currencyCode = invoiceReportRS.getString("ISO_Currency_Code");
                    String attachmentDate = invoiceReportRS.getString("Attachment_Date");
                    String location = invoiceReportRS.getString("Location");
                    String name = invoiceReportRS.getString("name");                
                    // get report filename
                    String reportFileName = getFilename(location);
                    // determine report path
                    String reportFileFullPath = getReportFilePath(location);
                    // check if report exists
                    File reportFile = new File(reportFileFullPath);
                    if (reportFile.exists())
                    {
                        // copy file to output location                    
                        String copyFileName =  outDir + File.separator + reportFileName;
                        if (billingSource.startsWith(MOBI))
                        {
                            // add invoice no to MOBI report name to make unique
                            reportFileName = "MOBI_inv_rpt_" + invoiceNo + PDFEXT;
                            copyFileName = outDir + File.separator + reportFileName;
                        }
                        if ((billingSource.startsWith(ARBOR))||(billingSource.startsWith(IVIW)))
                        {
                           // check for non-unique Arbor report name 
                           // if found add invoice number 
                           if ((reportFileName.startsWith(ARBORSTDINVOICE))||
                               (reportFileName.startsWith(ARBORSTDDETAIL))||
                               (reportFileName.startsWith(ARBORSTDCREDIT))) 
                           {
                                reportFileName = 
                                   reportFileName.substring(0, reportFileName.length() - 4) +
                                   USCORE +
                                   invoiceNo.replace("/", "-") +
                                   PDFEXT;
                                copyFileName = outDir + File.separator + reportFileName;
                           }
                        }
                        // Check if filename is a duplicate
                        // If it is add account number to filename 
                        File dupFile = new File(copyFileName);
                        if (dupFile.exists())
                        {
                          reportFileName = 
                               reportFileName.substring(0, reportFileName.length() - 4) +
                               USCORE +
                               accountNumber +
                               PDFEXT; 
                          copyFileName = outDir + File.separator + reportFileName;
                          invoiceNo = invoiceNo + USCORE + accountNumber;
                        }
                        File copyFile = new File(copyFileName);
                        InputStream is = null;
                        OutputStream os = null;
                        try
                        {
                            is = new FileInputStream(reportFile);
                            os = new FileOutputStream(copyFile);
                            byte[] buffer = new byte[1024];
                            int length;
                            while ((length = is.read(buffer)) > 0) 
                            {
                                os.write(buffer, 0, length);
                            }
                            is.close();
                            os.close();
                            // write file details to the control file
                            writeToControlFile(
                                    formatControlFileLine(
                                        accountNumber,
                                        accountName,
                                        invoiceNo,
                                        taxPointDate,
                                        periodFromDate,
                                        periodToDate,
                                        invoiceTotal,
                                        currencyCode,
                                        attachmentDate,
                                        name,
                                        reportFileName,
                                        REPORT));
                            controlCount++;
                            reportCount++;
                        }
                        catch(IOException ex)
                        {
                            message = 
                                "   I/O ERROR: Copying : " + 
                               reportFileName + " : to : " + 
                               copyFileName + " : "+ 
                               ex.getMessage();
                           outputMessage(message);
                           writeToErrorFile(location);
                           errorCount++;
                           reportErrorCount++;
                        }
                    }
                    else
                    {
                        // file not found so write location to error file
                        writeToErrorFile(location);
                        errorCount++;
                    }
                }
            }
            catch(java.sql.SQLException ex)
            {
                String ioMessage = ex.getMessage();
                if (!ioMessage.startsWith("Io exception: Connection reset"))
                {
                    message = 
                    "   DB ERROR: Failed to process invoiceReportRS "+
                    ex.getMessage(); 
                    outputMessage(message);
                }

            }
            invoiceReportRS = null;  
        } 
        else
        {
            message = "   DB ERROR: Failed to connect to Oracle for monthly CDR file processing";
            outputMessage(message);
        }
        
        // process monthly CDRs
        if (createOracleObject())
        {            
            ResultSet monthlyCDRRS = oDB.getMonthlyCDRs(billingSource, startDate, endDate);
            try
            {
                while(monthlyCDRRS.next())
                {
                    // Store invoice report values
                    String accountNumber = monthlyCDRRS.getString("Account_Number");
                    String accountName = monthlyCDRRS.getString("Account_Name");
                    String invoiceNo = monthlyCDRRS.getString("Invoice_No");
                    String taxPointDate = monthlyCDRRS.getString("Tax_Point_Date");
                    String periodFromDate = monthlyCDRRS.getString("Period_From_Date");
                    String periodToDate = monthlyCDRRS.getString("Period_To_Date");
                    String invoiceTotal = monthlyCDRRS.getString("Invoice_Total");
                    String currencyCode = monthlyCDRRS.getString("ISO_Currency_Code");
                    String attachmentDate = monthlyCDRRS.getString("Attachment_Date");
                    String location = monthlyCDRRS.getString("Location");
                    String name = monthlyCDRRS.getString("name");                
                    // get report filename
                    String cdrFileName = getFilename(location);
                    // determine report path
                    String cdrFileFullPath = getReportFilePath(location);
                    // check if report exists
                    File cdrFile = new File(cdrFileFullPath);
                    if (cdrFile.exists())
                    {
                        // copy file to output location                    
                        String copyFileName =  outDir + File.separator + cdrFileName;
                        File copyFile = new File(copyFileName);
                        InputStream is = null;
                        OutputStream os = null;
                        try
                        {
                            is = new FileInputStream(cdrFile);
                            os = new FileOutputStream(copyFile);
                            byte[] buffer = new byte[1024];
                            int length;
                            while ((length = is.read(buffer)) > 0) 
                            {
                                os.write(buffer, 0, length);
                            }
                            is.close();
                            os.close();
                            // write file details to the control file
                            writeToControlFile(
                                    formatControlFileLine(
                                        accountNumber,
                                        accountName,
                                        invoiceNo,
                                        taxPointDate,
                                        periodFromDate,
                                        periodToDate,
                                        invoiceTotal,
                                        currencyCode,
                                        attachmentDate,
                                        name,
                                        cdrFileName,
                                        MONTHLYCDR));
                            controlCount++;
                            monthlyCDRCount++;
                        }
                        catch(IOException ex)
                        {
                            message = 
                                "   I/O ERROR: Copying : " + 
                               cdrFileName + " : to : " + 
                               copyFileName + " : "+ 
                               ex.getMessage();
                           outputMessage(message);
                           writeToErrorFile(location);
                           errorCount++;
                           monthlyCDRErrorCount++;
                        }
                    }
                    else
                    {
                        // file not found so write location to error file
                        writeToErrorFile(location);
                        errorCount++;
                    }
                }
            }
            catch(java.sql.SQLException ex)
            {
                String ioMessage = ex.getMessage();
                if (!ioMessage.startsWith("Io exception: Connection reset"))
                {
                    message = 
                    "   DB ERROR: Failed to process monthlyCDRRS "+
                    ex.getMessage(); 
                    outputMessage(message);
                }
            }
            monthlyCDRRS = null;  
        } 
        else
        {
            message = "   DB ERROR: Failed to connect to Oracle for monthly CDR file processing";
            outputMessage(message);
        }
                
        // process daily CDRs
        if (createOracleObject())
        {            
            ResultSet dailyCDRRS = oDB.getDailyCDRs(billingSource, startDate, endDate);
            try
            {
                while(dailyCDRRS.next())
                {
                    // Store invoice report values
                    String accountNumber = dailyCDRRS.getString("Account_Number");
                    String accountName = dailyCDRRS.getString("Account_Name");
                    String attachmentDate = dailyCDRRS.getString("Attachment_Date");
                    String location = dailyCDRRS.getString("Location");
                    String name = dailyCDRRS.getString("name");                
                    // get report filename
                    String cdrFileName = getFilename(location);
                    // determine report path
                    String cdrFileFullPath = getReportFilePath(location);
                    // check if report exists
                    File cdrFile = new File(cdrFileFullPath);
                    if (cdrFile.exists())
                    {
                        // copy file to output location                    
                        String copyFileName =  outDir + File.separator + cdrFileName;
                        File copyFile = new File(copyFileName);
                        InputStream is = null;
                        OutputStream os = null;
                        try
                        {
                            is = new FileInputStream(cdrFile);
                            os = new FileOutputStream(copyFile);
                            byte[] buffer = new byte[1024];
                            int length;
                            while ((length = is.read(buffer)) > 0) 
                            {
                                os.write(buffer, 0, length);
                            }
                            is.close();
                            os.close();
                            // write file details to the control file
                            writeToControlFile(
                                    formatControlFileLine(
                                        accountNumber,
                                        accountName,
                                        "",
                                        "",
                                        "",
                                        "",
                                        "",
                                        "",
                                        attachmentDate,
                                        name,
                                        cdrFileName,
                                        DAILYCDR));
                            controlCount++;
                            dailyCDRCount++;
                        }
                        catch(IOException ex)
                        {
                            message = 
                                "   I/O ERROR: Copying : " + 
                               cdrFileName + " : to : " + 
                               copyFileName + " : "+ 
                               ex.getMessage();
                           outputMessage(message);
                           writeToErrorFile(location);
                           errorCount++;
                           dailyCDRErrorCount++;
                        }
                    }
                    else
                    {
                        // file not found so write location to error file
                        writeToErrorFile(location);
                        errorCount++;
                    }
                }
            }
            catch(java.sql.SQLException ex)
            {
                String ioMessage = ex.getMessage();
                if (!ioMessage.startsWith("Io exception: Connection reset"))
                {
                    message = 
                    "   DB ERROR: Failed to process dailyCDRRS "+
                    ex.getMessage(); 
                    outputMessage(message);
                }

            }
            dailyCDRRS = null;
        }
        else
        {
            message = "   DB ERROR: Failed to connect to Oracle for monthly CDR file processing";
            outputMessage(message);
        }
        
        // process attachments
        if (createOracleObject())
        {            
            ResultSet attachmentsRS = oDB.getAttachments(billingSource, startDate, endDate);
            try
            {
                while(attachmentsRS.next())
                {
                    // Store invoice report values
                    String accountNumber = attachmentsRS.getString("Account_Number");
                    String accountName = attachmentsRS.getString("Account_Name");
                    String invoiceNo = attachmentsRS.getString("Invoice_No");
                    String taxPointDate = attachmentsRS.getString("Tax_Point_Date");
                    String periodFromDate = attachmentsRS.getString("Period_From_Date");
                    String periodToDate = attachmentsRS.getString("Period_To_Date");
                    String invoiceTotal = attachmentsRS.getString("Invoice_Total");
                    String currencyCode = attachmentsRS.getString("ISO_Currency_Code");
                    String attachmentDate = attachmentsRS.getString("Attachment_Date");
                    String location = attachmentsRS.getString("Location");
                    String name = attachmentsRS.getString("name");                
                    // get report filename
                    String reportFileName = getFilenameAttachment(location);
                    // determine report path
                    String reportFileFullPath = getReportFilePath(location);
                    // check if report exists
                    File reportFile = new File(reportFileFullPath);
                    if (reportFile.exists())
                    {
                        // copy file to output location 
                        String copyFileName = outDir + File.separator + reportFileName;
                        File copyFile = new File(copyFileName);
                        InputStream is = null;
                        OutputStream os = null;
                        try
                        {
                            is = new FileInputStream(reportFile);
                            os = new FileOutputStream(copyFile);
                            byte[] buffer = new byte[1024];
                            int length;
                            while ((length = is.read(buffer)) > 0) 
                            {
                                os.write(buffer, 0, length);
                            }
                            is.close();
                            os.close();
                            // write file details to the control file
                            writeToControlFile(
                                    formatControlFileLine(
                                        accountNumber,
                                        accountName,
                                        invoiceNo,
                                        taxPointDate,
                                        periodFromDate,
                                        periodToDate,
                                        invoiceTotal,
                                        currencyCode,
                                        attachmentDate,
                                        name,
                                        reportFileName,
                                        ATTACHMENT));
                            controlCount++;
                            attachmentCount++;
                        }
                        catch(IOException ex)
                        {
                            message = 
                                "   I/O ERROR: Copying : " + 
                               reportFileName + " : to : " + 
                               copyFileName + " : "+ 
                               ex.getMessage();
                           outputMessage(message);
                           writeToErrorFile(location);
                           errorCount++;
                        }
                    }
                    else
                    {
                        // file not found so write location to error file
                        writeToErrorFile(location);
                        errorCount++;
                        attachmentErrorCount++;
                    }
                }
            }
            catch(java.sql.SQLException ex)
            {
                String ioMessage = ex.getMessage();
                if (!ioMessage.startsWith("Io exception: Connection reset"))
                {
                    message = 
                    "   DB ERROR: Failed to porcess attachmentsRS "+
                    ex.getMessage(); 
                    outputMessage(message);
                }
            }
            attachmentsRS = null;  
        } 
        else
        {
            message = "   DB ERROR: Failed to connect to Oracle for monthly CDR file processing";
            outputMessage(message);
        }
        
        // write control file footer
        writeToControlFile("Total = "+controlCount);
        outputMessage("   ");
        outputMessage("   Billing Source  : " + billingSource);
        outputMessage("   Start Date      : " + startDate);
        outputMessage("   End Date        : " + endDate);
        outputMessage("   ");
        outputMessage("      Reports delivered      : " + reportCount);
        outputMessage("      Monthly CDRs delivered : " + monthlyCDRCount);
        outputMessage("      Daily CDRs delivered   : " + dailyCDRCount);
        outputMessage("      Attachments delivered  : " + attachmentCount);
        outputMessage("   ");
        outputMessage("   Total files delivered     : " + controlCount);
        if ((reportErrorCount>0)||
            (monthlyCDRErrorCount>0)||
            (dailyCDRErrorCount>0)||
            (attachmentErrorCount>0))
            outputMessage("   ");            
        if (reportErrorCount>0)
            outputMessage("      Missing reports        : " + reportErrorCount);
        if (monthlyCDRErrorCount>0)
            outputMessage("      Missing Monthly CDRs   : " + monthlyCDRErrorCount);
        if (dailyCDRErrorCount>0)
            outputMessage("      Missing Daily CDRs     : " + dailyCDRErrorCount);
        if (attachmentErrorCount>0)
            outputMessage("      Missing attachments    : " + attachmentErrorCount);
        if (errorCount>0)
        {
            outputMessage("   ");
            outputMessage("   Total missing files       : " + errorCount);
        }
        outputMessage("   ");
        
        // write error file footer
        writeToErrorFile("Total = "+errorCount);
        
        // close control and error files
        closeControlFile();
        closeErrorFile(); 
    }
    
    private String formatControlFileLine
    (   String accountNumber,
        String accountName,
        String invoiceNumber,
        String taxPointDate,
        String periodFromDate,
        String periodToDate,
        String invoiceTotal,
        String currencyCode,
        String attachmentDate,
        String name,
        String filename,
        String filetype )
    {
        String line = "";
        String displayName = name;
        
        // Amend name for Arbor reports
        if (((billingSource.equals(ARBOR))||(billingSource.equals(IVIW))) && 
            (filetype.equals(REPORT)) &&
            (name.startsWith(INVOICE)))
            displayName = INVOICE;
        
        if (((billingSource.equals(ARBOR))||(billingSource.equals(IVIW))) && 
            (filetype.equals(REPORT)) &&
            (name.startsWith(INVOICEDETAIL)))
            displayName = OTHERREPORTS;
        
        // Amend name for Arbor monthly CDRs
        if ((billingSource.equals(ARBOR)) && 
            (filetype.equals(MONTHLYCDR)))
            {
                if (name.startsWith(ARBORMONTHLYINVOICEDCDRSPART))
                    displayName = MONTHLYCDRSPART + name.substring(name.length()-1,name.length());
                else if (name.startsWith(ARBORMONTHLYINVOICEDCDRS))
                    displayName = MONTHLYCDRS;
                else if (name.startsWith(ARBORMONTHLYCDRSUMMARIES))
                    displayName = MONTHLYCDRSUMMARIES;
                else if ((name.startsWith("IP"))&&(!name.endsWith(IPTRUNKINGCDRS)))
                    displayName = MONTHLYIPTRUNKINGCDRS + " (Manual)";
                else if (name.startsWith(IPTRUNKINGCDRS))
                    displayName = MONTHLYIPTRUNKINGCDRS;
                else
                    displayName = MONTHLYCDRS + " (Manual)";                
            }
        
        // Amend name for Arbor daily CDRs
        if ((billingSource.equals(ARBOR)) && 
            (filetype.equals(DAILYCDR)))
            displayName = DAILYCDRS; 
        
        // Amend name for ROI monthly CDRs
        if ((billingSource.equals(ROI)) && 
            (filetype.equals(MONTHLYCDR)))
            {
                if (name.startsWith(MONTHLYCDRSROI))
                    displayName = MONTHLYCDRS;
                else if (name.startsWith(MONTHLYWLRCDRSROI))
                    displayName = MONTHLYWLRCDRS;
                else
                    displayName = MONTHLYCDRS + " (Manual)";                
            }
        
        // Amend name for ROI daily CDRs
        if ((billingSource.equals(ROI)) && 
            (filetype.equals(DAILYCDR)))
            displayName = DAILYCDRS;
        
        // Amend name for BASA reports
        if (((billingSource.startsWith(BASA))||(billingSource.startsWith(VBS))) && 
            (filetype.startsWith(REPORT)))
            if (name.endsWith(DETAIL))
            {
                displayName = OTHERREPORTS;
            }    
            else
            {
                if (name.substring(name.length()-3,name.length()-2).equals("0"))
                    displayName = INVOICE + " " + name.substring(name.length()-3, name.length());
                else
                    displayName = INVOICE;
            }
        
        // Amend name for GCB1 reports
        if ((billingSource.startsWith(GCB1)) && 
            (filetype.startsWith(REPORT)))
            if ((name.startsWith(INVOICE))||(name.endsWith(INVOICE)))
                displayName = INVOICE;
            else
                if (name.startsWith(SERVICESUMMARY))
                    displayName = SERVICESUMMARY;
                else
                    displayName = SERVICEREFERENCEDETAIL;
        
        // Amend name for GCB3 reports
        if ((billingSource.startsWith(GCB3)) && 
            (filetype.startsWith(REPORT)))
        {            
            if ((name.endsWith(INVOICE)))
                displayName = INVOICE;         
            if ((name.endsWith(INVOICEQUARTERLY)))
                displayName = INVOICEQUARTERLY;
        }  
        
        // Amend name for GCB5 reports
        if ((billingSource.startsWith(GCB5)) && 
            (filetype.startsWith(REPORT))) 
            displayName = INVOICE;
        
        // Amend name for GCB7 reports
        if ((billingSource.startsWith(GCB7)) &&
            (filetype.startsWith(REPORT)))
        {            
            if ((name.endsWith(INVOICE)))
                displayName = INVOICE;         
            else
                displayName = SERVICESUMMARY;
        }
        
        // Amend name for Storm reports
        if ((billingSource.startsWith(STORM)) && 
            (filetype.startsWith(REPORT))) 
            displayName = INVOICE;
        
        // Amend name for MOBI reports
        if ((billingSource.startsWith(MOBI)) && 
            (filetype.startsWith(REPORT))) 
            displayName = INVOICE;    
        
        // Amend name for RSMA monthly CDRs
        if ((billingSource.equals(RSMA)) && 
            (filetype.equals(MONTHLYCDR)))
            {
                if ((name.startsWith(INVOICEBACKUP))&&(name.endsWith(INVOICEBACKUP)))
                    displayName = MONTHLYCDRS;
                else
                    displayName = MONTHLYCDRS + " (Manual)";                
            }
        
        // Amend name for SSBS monthly CDRs
        if ((billingSource.equals(SSBS)) && 
            (filetype.equals(MONTHLYCDR)))
            {
                if (name.startsWith(SSBSMONTHLYINVOICEDPSTNCDRSPART))
                    displayName = MONTHLYPSTNCDRSPART + name.substring(name.length()-1,name.length());
                else if ((name.startsWith(SSBSMONTHLYINVOICEDPSTNCDRS))&&(name.endsWith(SSBSMONTHLYINVOICEDPSTNCDRS)))
                    displayName = MONTHLYPSTNCDRS;
                else if (name.startsWith(SSBSMONTHLYINVOICEDPSTNSUMMARIES))
                    displayName = MONTHLYPSTNCDRSUMMARIES;
                else if (name.startsWith(SSBSMONTHLYINVOICEDTNBSCDRSPART))
                    displayName = MONTHLYTNBSCDRSPART + name.substring(name.length()-1,name.length());
                else if ((name.startsWith(SSBSMONTHLYINVOICEDTNBSCDRS))&&(name.endsWith(SSBSMONTHLYINVOICEDTNBSCDRS)))
                    displayName = MONTHLYTNBSCDRS;
                else if (name.startsWith(SSBSMONTHLYINVOICEDTNBSSUMMARIES))
                    displayName = MONTHLYTNBSCDRSUMMARIES;
                else if (name.startsWith(SSBSENDOFMONTHTNBSCDRSPART))
                    displayName = ENDOFMONTHTNBSCDRSPART + name.substring(name.length()-1,name.length());
                else if ((name.startsWith(SSBSENDOFMONTHTNBSCDRS))&&(name.startsWith(SSBSENDOFMONTHTNBSCDRS)))
                    displayName = ENDOFMONTHTNBSCDRS;
                else if (name.startsWith(SSBSENDOFMONTHTNBSSUMMARIES))
                    displayName = ENDOFMONTHTNBSCDRSUMMARIES;
                else if (name.startsWith(SSBSALARMCALL))
                    displayName = ALARMCALLS;
                else
                    displayName = MONTHLYCDRS + " (Manual)";                
            }
        
        // Amend name for SSBS daily CDRs
        if ((billingSource.equals(SSBS)) && 
            (filetype.equals(DAILYCDR)) &&
             (name.startsWith(SSBSDAILYPSTNCDRS)))
            displayName = DAILYPSTNCDRS;
        if ((billingSource.equals(SSBS)) && 
            (filetype.equals(DAILYCDR)) &&
             (name.startsWith(SSBSDAILYTNBSCDRS)))
            displayName = DAILYTNBSCDRS;
        
        // Amend name for Storm monthly CDRs
        if ((billingSource.equals(STORM)) && 
            (filetype.equals(MONTHLYCDR)) )
        {
            if ((name.startsWith(STORMMONTHLYCDRS))&&(name.endsWith(STORMMONTHLYCDRS)))
                displayName = MONTHLYCDRS;
            else
                displayName = MONTHLYCDRS  + " (Manual)";
        }             
        
        // Amend name for Storm daily CDRs
        if ((billingSource.equals(STORM)) && 
            (filetype.equals(DAILYCDR)))
        {
            if ((name.startsWith(STORMDAILYCDRS))&&(name.endsWith(STORMDAILYCDRS)))
                displayName = DAILYCDRS;
            else
                displayName = DAILYCDRS  + " (Manual)";
        }    
        
        // Format control file line
        line =
            bSource+ COMMA + 
            getAMLBillingSource(bSource,accountNumber) + COMMA +    
            accountNumber + COMMA +
            accountName + COMMA +
            invoiceNumber + COMMA +
            taxPointDate + COMMA +
            periodFromDate + COMMA +
            periodToDate + COMMA +
            invoiceTotal + COMMA +
            currencyCode + COMMA +
            attachmentDate + COMMA +
            displayName + COMMA +
            filename;                
        return line;
    }
    
    private String getAMLBillingSource(String billingSource, String accountNumber )
    {
        // No change required for PCB, QB, SSBS BASA or VBS as AML Billing Source is same
        String AMLBillingSource = billingSource;
        // Amend Arbor and IVIW to PESP
        if ((billingSource.startsWith(ARBOR))||(billingSource.startsWith(IVIW)))
            AMLBillingSource = PESP;
        // Amend GCB1, GCB3, GCB5 and GCB7 to GCBR
        if ((billingSource.startsWith(GCB1OUT))||
            (billingSource.startsWith(GCB3OUT))||
            (billingSource.startsWith(GCB5OUT))||
            (billingSource.startsWith(GCB7OUT)))
            AMLBillingSource = GCBR;
        // Amend MOBI to DNS
        if (billingSource.startsWith(MOBI))
            AMLBillingSource = DNS;
        // Amend RSMA to SSBS
        if (billingSource.startsWith(RSMAOUT))
            AMLBillingSource = SSBS;
        // Amend Storm to STRM
        if (billingSource.startsWith(STORM))
            AMLBillingSource = STRM;
        // Amend to MOVE for ROI accounts starting with 'C' 
        // and EBLZ for others ending in IR or UK
        if (billingSource.startsWith(ROIOUT))
            if (accountNumber.startsWith("C"))
                AMLBillingSource = MOVE;
            else
                AMLBillingSource = EBLZ;
        return AMLBillingSource;
    }
    
    private String getReportFilePath(String location)
    {
        String path = "";
        if (location.startsWith("archive"))
            path = archiveDir + File.separator + location.substring(8,location.length());
        else
            path = attachDir + File.separator + location;
        return path;
    }
    
    private String getFilename(String location)
    {
        String filename = "";
        // find last forward slash in filename
        int pos = location.length() -1;
        String testChar = location.substring(pos, pos+1);
        while (!testChar.equals(FWDSLASH))
        {
           pos = pos - 1; 
           testChar = location.substring(pos, pos+1); 
        }
        filename = location.substring(pos+1,location.length());
        return filename;
    }
    
    private String getFilenameAttachment(String location)
    {
        String filename = "";
        // find second to last forward slash in filename
        int pos = location.length() -1;
        String testChar = location.substring(pos, pos+1);
        while (!testChar.equals(FWDSLASH))
        {
           pos = pos - 1; 
           testChar = location.substring(pos, pos+1); 
        }
        pos = pos - 1; 
        testChar = location.substring(pos, pos+1);
        while (!testChar.equals(FWDSLASH))
        {
           pos = pos - 1; 
           testChar = location.substring(pos, pos+1); 
        }
        filename = location.substring(pos+1,location.length()).replaceAll(FWDSLASH, USCORE);
        return filename;
    }
    
    private void outputMessage(String inMessage)
    {
        System.out.println(inMessage.trim());
        writeToLogFile(inMessage);
    }
    
    private boolean validBillingSource(String inBillSrc)
    {
        boolean result = false;
        // check that supplied billing source is one of the expected values
        if ( (inBillSrc.equals(ARBOR)) || 
             (inBillSrc.equals(BASA))  ||
             (inBillSrc.equals(ROI))   ||
             (inBillSrc.equals(GCB1))  ||
             (inBillSrc.equals(GCB3))  ||
             (inBillSrc.equals(GCB5))  ||
             (inBillSrc.equals(GCB7))  ||
             (inBillSrc.equals(IVIW))  ||
             (inBillSrc.equals(MOBI))  ||
             (inBillSrc.equals(PCB))   ||
             (inBillSrc.equals(QB))    ||
             (inBillSrc.equals(RSMA))  ||
             (inBillSrc.equals(SSBS))  ||
             (inBillSrc.equals(STORM)) ||
             (inBillSrc.equals(VBS)) )
            result = true;
        return result;
    }
    
        // open log file
    private void openLogFile()
    {
        logBW = null;
        String logDate = new java.util.Date().toString();
        String logFilename = 
            logDir+File.separator+"attachmentExtract_"+
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
    
    // open control file
    private void openControlFile()
    {
        controlFileBW = null;
        String controlFileDate = new java.util.Date().toString();
        String controlFileName = 
            outDir+
            File.separator+
            bSource+
            USCORE+
            startDate+
            USCORE+
            endDate+ 
            USCORE+   
            controlFileDate.substring(24,28)+
            decodeMonth(controlFileDate.substring(4,7))+
            controlFileDate.substring(8,10)+
            USCORE+
            controlFileDate.substring(11,13)+
            controlFileDate.substring(14,16)+
            controlFileDate.substring(17,19)+
            USCORE+
            CONTROL+
            CSVTEXT;
        try
        {
            controlFileBW = new BufferedWriter(new FileWriter(controlFileName));
        }
        catch (IOException ex)
        {
            outputMessage("   Error opening control file : "+ ex.getMessage());
        }
    }   
        
    // write line to control file
    private void writeToControlFile( String line )
    {
        try
        {
            controlFileBW.write(line+"\r\n");
        }
        catch (IOException ex)
        {
            outputMessage("   Error writing to control file : "+  ex.getMessage());
        }
    }   
    
    // close control file
    private void closeControlFile()
    {
        try
        {
            controlFileBW.close();
        }
        catch (IOException ex)
        {
            outputMessage("   Error closing control file : "+ ex.getMessage());
        }
    } 
        
    // open error file
    private void openErrorFile()
    {
        errorFileBW = null;
        String errorFileDate = new java.util.Date().toString();
        String errorFileName = 
            outDir+
            File.separator+
            bSource+
            USCORE+
            startDate+
            USCORE+
            endDate+ 
            USCORE+   
            errorFileDate.substring(24,28)+
            decodeMonth(errorFileDate.substring(4,7))+
            errorFileDate.substring(8,10)+
            USCORE+
            errorFileDate.substring(11,13)+
            errorFileDate.substring(14,16)+
            errorFileDate.substring(17,19)+
            USCORE+
            ERROR+
            TEXTEXT;
        try
        {
            errorFileBW = new BufferedWriter(new FileWriter(errorFileName));
        }
        catch (IOException ex)
        {
            outputMessage("   Error opening error file : "+ ex.getMessage());
        }
    }   
        
    // write line to error file
    private void writeToErrorFile( String line )
    {
        try
        {
            errorFileBW.write(line+"\r\n");
        }
        catch (IOException ex)
        {
            outputMessage("   Error writing to error file : "+  ex.getMessage());
        }
    }   
    
    // close error file
    private void closeErrorFile()
    {
        try
        {
            errorFileBW.close();
        }
        catch (IOException ex)
        {
            outputMessage("   Error closing error file : "+ ex.getMessage());
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

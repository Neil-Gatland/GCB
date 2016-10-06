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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class crystalExtract 
{    
    // oracle connection variables
    private String dbServer, service, username, password;
    private Connection conn;
    // OracleDB class variables
    private oracleDB oDB;
    // file i/o variables
    private String logDir, inDir, outDir;    
    private BufferedWriter logBW, controlFileBW, errorFileBW;
    // run parameter variables
    private String billingSource, startDate, endDate, bSource;
    // logging variables
    private String message;
    // String constants    
    private final String USCORE = "_";
    private final String COMMA = ",";
    private final String SPACE = " ";
    private final String FWDSLASH = "/";
    private final String HYPHEN = "-";
    private final String INVOICE = "Invoice";
    private final String LOG = "log";
    private final String ERROR = "error";
    private final String CONTROL = "control";
    private final String NOTFOUND = "NOT FOUND";
    private final String QUARTERLYSUFFIX = " (Quarterly)";
    private final String CONTROLHEADER = 
        "Billing Source,AML Billing Source,Account Number,Account Name,"+
        "Invoice No,Tax Point Date,Period From Date,Period To Date,"+
        "Invoice Total,Currency Code,Attachment_Date,File Type,File Name";
    // File extensions
    private final String CSVTEXT = ".csv";
    private final String TEXTEXT = ".txt";
    private final String PDFEXT = ".pdf";
    // ebilling Billing Sources
    private final String GCB1 = "GCB Data";
    private final String GCB3 = "Conglom";
    private final String GCB5 = "MS";
    private final String GCB7 = "Globaldial";
    private final String GCB1OUT = "GCB1";
    private final String GCB3OUT = "GCB3";
    private final String GCB5OUT = "GCB5";
    private final String GCB7OUT = "GCB7";
    private final String GSO = "GSO";
    private final String MMS = "MMS";
    private final String NOSTRO = "Nostro";
    private final String TB = "TeleBusiness";
    // AML Billing Sources
    private final String GCBR = "GCBR";
    private final String GCBW = "GCBW";
    private final String SSBS = "SSBS";
    // error messages
    private final String INVOICEMISSING = "Invoice not found";
    private final String WRONGBILLINGSOURCE = "Wrong Billing Source";
    private final String NOTINDATERANGE = "Not in date range";
    private final String MOVEFAILED = "Failed to move renamed file";
    // Crystal report types
    private final String DATAINVOICERPT = "0._Data_Services_Invoice";
    private final String STRATEGICINVOICERPT = "0._Strategic_Invoice";
    private final String MANAGEDSERVICESINVOICERPT = "0._Managed_Services_Invoice";
    private final String GCB1BILLINGREGIONSUMMARYRPT = 
        "1._Data_Summary_Global_Charges_Report_Grouped_By_Billing_Region";
    private final String STRATEGICPNN3DETAILRPT = "1._Strategic_PNN3_Detail";
    private final String GCB1SERVICEREFERENCESUMMARYRPT = "2._Data_Services_Summary_Billing_Report";
    private final String GCB1SITEDETAILRPT = "3._Data_Services_Detailed_Billing_Report";
    private final String GCB1SERVICEREFERENCEDETAILRPT = "4._Data_Services_Detailed_Billing_Report";
    private final String GCB1TAXSUMMARYRPT = "4._Data_Services_Summary_Tax_Report";
    private final String GCB1TAXDETAILRPT = "5._Data_Services_Detailed_Tax_Report";
    private final String GCB1TAXRPT = "5._Data_Tax_Report";
    private final String GCB1SERVICEREFERENCESUMMARYALTRPT = "D2._Data_Services_Summary_Billing_Report";
    private final String GCB1SERVICEREFERENCEDETAILALTRPT = "D3._Data_Services_Detailed_Billing_Report";
    private final String GCB1CPSVOICEUSAGESUMMARYPT = "13._CPS_Voice_Summary";    
    private final String GCB3INVOICERPT = "0._Conglomerate_Invoice";   
    private final String GCB3INVOICEQUARTERLYRPT = "0._Conglomerate_Invoice_-_QBMR";
    private final String GCB3LEASEDCONSOLSUMMARYRPT = "L1._Consolidated_Summary";
    private final String GCB3LEASEDCONSOLSUMMARYBYDRPT = "L1._(BYD)_Consolidated_Summary";
    private final String GCB3LEASEDCONSOLSUMMARYQBMRRPT = "L1._Consolidated_Summary_-_QBMR";
    private final String GCB3LEASEDACREDITDJUSTMENTSRPT = "L3._Credit_Adjustments";
    private final String GCB3LEASEDRENTALSRPT = "L4._Rentals";
    private final String GCB3LEASEDRENTALSBYDRPT = "L4._(BYD)_Rentals";
    private final String GCB3LEASEDSUNDRYCHARGESRPT = "L5._Sundry_Charges";
    private final String GCB3LEASEDINSTALLATIONCHARGESRPT = "L6._Installation_Charges";
    private final String GCB3SWITCHEDCONSOLSUMMARY1RPT ="S1._Consolidated_Summary";
    private final String GCB3SWITCHEDCONSOLSUMMARY1QBMRRPT ="S1._Consolidated_Summary_QBMR";
    private final String GCB3SWITCHEDCONSOLSUMMARY2RPT ="S2._Consolidated_Summary";
    private final String GCB3SWITCHEDCONSOLSUMMARY2QBMRRPT ="S2._Consolidated_Summary_QBMR";
    private final String GCB3SWITCHEDCONSOLSUMMARY2WOLRPT ="S2._(WOL)_Consolidated_Summary";
    private final String GCB3SWITCHEDCONSOLSUMMARY2BYDRPT ="S2._(BYD)_Consolidated_Summary";
    private final String GCB3SWITCHEDEASYACCESSQUARTERLYRPT ="S11._Easy_Access_Quarterly_Charges";
    private final String GCB3SWITCHEDEASYACCESSUSAGERPT ="S12._Easy_Access_Usage_Charges";
    private final String GCB3SWITCHEDINSTALLATIONCHARGESRPT ="S13._Installation_Charges";
    private final String GCB3SWITCHEDINSTALLATIONCHARGESBYDRPT ="S13._(BYD)_Installation_Charges";
    private final String GCB3SWITCHEDRENTALCHARGESRPT ="S14._Rental_Charges";
    private final String GCB3SWITCHEDRENTALCHARGESBYDRPT ="S14._(BYD)_Rental_Charges";
    private final String GCB3SWITCHEDSOURCEDISCOUNTSRPT ="S15._Source_Discounts";
    private final String GCB3SWITCHEDSOURCEDISCOUNTSBYDRPT ="S15._(BYD)_Source_Discounts";
    private final String GCB3SWITCHEDSPECIALCHARGESRPT ="S16._Special_Charges_Summary";
    private final String GCB3SWITCHEDSUNDRYCHARGESRPT ="S.17_Sundry_Charges";
    private final String GCB3SWITCHEDSUNDRYCHARGESBYDRPT ="S17._(BYD)_Sundry_Charges";
    private final String GCB3SWITCHEDUSAGESUMMARY18RPT ="S18._Usage_Summary";
    private final String GCB3SWITCHEDUSAGESUMMARY19RPT ="S19._Usage_Summary";
    private final String GCB3SWITCHEDVPNCHARGESRPT ="S20._VPN_Charges_Summary";
    private final String GCB3SWITCHEDAUTHCODECHARGESRPT ="S3._Authorisation_Code_Charges";
    private final String GCB3SWITCHEDBILLINGNUMBERCHARGES4RPT ="S4._Billing_Number_Charge";
    private final String GCB3SWITCHEDBILLINGNUMBERCHARGES4BYDRPT ="S4._(BYD)_Billing_Number_Charge";
    private final String GCB3SWITCHEDBILLINGNUMBERCHARGES5RPT ="S5._Billing_Number_Charge";
    private final String GCB3SWITCHEDBILLINGNUMBERCHARGES52RPT ="S5_2._Billing_Number_Charge";
    private final String GCB3SWITCHEDBILLINGNUMBERCHARGES4WOLRPT ="S5._(WOL)_Billing_Number_Charge";
    private final String GCB3SWITCHEDCALLCHARGESSUMMARYRPT ="S6._Call_Charges_Summary";
    private final String GCB3SWITCHEDCALLCHARGESSUMMARYBYDRPT ="S6._(BYD)_Call_Charges_Summary";
    private final String GCB3SWITCHEDCALLINKCHARGESRPT ="S7._Callink_Charges";
    private final String GCB3SWITCHEDCALLINKCHARGESBYDRPT ="S7._(BYD)_Callink_Charges";
    private final String GCB3SWITCHEDACREDITDJUSTMENTSRPT = "S8._Credits_and_Adjustments";
    private final String GCB5INVOICERPT = "0._MS_Invoice";
    private final String GCB5INVOICEBELRPT = "0._MS_Invoice_(Belgacom)";
    private final String GCB7STRATEGICGLOBALDIALDETAIL = "1._Strategic_Globaldial_Detail";
    private final String GSOINVOICERPT = "0._Invoice_by_Customer";
    private final String GSOSUMMARYSHEETRPT = "1._Summary_Invoice_Sheet_by_Customer";
    private final String GSODETAILEDBILLINGRPT = "2._Detailed_Billing_Report_by_Customer";
    private final String MMSINVOICERPT = "0._MMS_Invoice";
    private final String SYBASEINVOICERPT = "0._Sybase_Invoice";
    private final String MMSCHARGESSUMNMARYRPT = "1._MMS_Charges_Summary";
    private final String SYBASECHARGESSUMNMARYRPT = "1._Sybase_Charges_Summary";
    private final String MMSTERMINATIONSUMNMARYRPT = "1._MMS_Termination_Summary";
    private final String SYBASETERMINATIONSUMNMARYRPT = "1._Sybase_Termination_Summary";
    private final String MMSTRANSITSUMNMARYRPT = "1._MMS_Transit_Summary";
    private final String SYBASETRANSITSUMNMARYRPT = "1._Sybase_Transit_Summary";
    private final String NOSTROINVOICERPT = "0._Nostro_Invoice";
    private final String NOSTROSUMMARYRPT = "1._Nostro_Summary";
    private final String NOSTRODETAILRPT = "2._Nostro_Detail";
    private final String TBSUMMARYPT = "0._TeleBusiness_Summary";
    private final String TBCRSUMMARYPT = "1._TeleBusiness_CR_Summary";
    private final String TBCRDETAILRPT = "2._TeleBusiness_CR_Detail";
    private final String PREMIUMRATESUMMARY = "Premium Rate Summary";
    private final String PREMIUMRATECALLSUMMARY = "Premium Rate Call Summary";
    private final String PREMIUMRATECALLDETAIL = "Premium Rate Call Detail";
    // File type values
    private final String DATAINVOICE = "Data Invoice";
    private final String STRATEGICINVOICE = "Strategic Invoice";
    private final String MANAGEDSERVICESINVOICE = "Managed Services Invoice";
    private final String BILLINGREGIONSUMMARY = "Billing Region Summary";
    private final String SERVICESUMMARY = "Service Summary";
    private final String SERVICEREFERENCESUMMARY = "Service Reference Summary";
    private final String SITEDETAIL = "Site Detail";
    private final String SERVICEREFERENCEDETAIL = "Service Reference Detail";
    private final String TAXSUMMARY = "Tax Summary";
    private final String TAXDETAIL = "Tax Detail";
    private final String TAX = "Tax";
    private final String CPSVOICEUSAGESUMMARY = "CPS Voice Usage Summary";
    private final String LEASEDCONSOLSUMMARY = "Leased - Consolidated Summary";
    private final String LEASEDCREDITANDADJUSTMENTS = "Leased - Credits and Adjustments";
    private final String LEASEDRENTALS = "Leased - Rentals";
    private final String LEASEDSUNDRYCHARGES = "Leased - Sundry Charges";
    private final String LEASEDINSTALLATIONCHARGES = "Leased - Installation Charges";
    private final String SWITCHEDCONSOLSUMMARY = "Switched - Consolidated Summary";
    private final String SWITCHEDEASYACCESSQUARTERLY = "Switched - Easy Access Quarterly";
    private final String SWITCHEDEASYACCESSUSAGE = "Switched - Easy Access Usage";
    private final String SWITCHEDINSTALLATIONCHARGES = "Switched - Installation Charges";
    private final String SWITCHEDRENTALCHARGES = "Switched - Rentals";
    private final String SWITCHEDSOURCEDISCOUNTS = "Switched - Source Discounts";
    private final String SWITCHEDSPECIALCHARGES = "Switched - Special Charges";
    private final String SWITCHEDSUNDRYCHARGES = "Switched - Sundry Charges";
    private final String SWITCHEDUSAGESUMMARY = "Switched - Usage Summary";
    private final String SWITCHEDVPNCHARGES = "Switched - VPN Charges";    
    private final String SWITCHEDAUTHCODECHARGES = "Switched - Authorisation Code Charges";   
    private final String SWITCHEDBILLINGNUMBERCHARGES = "Switched - Billing Number Charges";  
    private final String SWITCHEDCALLCHARGESSUMMARY = "Switched - Call Charges Summary";      
    private final String SWITCHEDCALLINKCHARGES = "Switched - Callink Charges"; 
    private final String SWITCHEDCREDITANDADJUSTMENTS = "Switched - Credits and Adjustments"; 
    private final String SUMMARYSHEET = "Summary Sheet"; 
    private final String DETAILEDBILLING = "Detailed Billing"; 
    private final String CHARGESSUMMARY = "Charges Summary";
    private final String TERMINATIONSUMMARY = "Termination Summary";
    private final String TRANSITSUMMARY = "Transit Summary";
    private final String SUMMARY = "Summary";
    private final String DETAIL = "Detail";

    public static void main(String[] args)
    {
        // control processing
        crystalExtract cE = new crystalExtract();
        cE.control();
    }   
        
    private void control()
    {
        // get relevant property values
        try
        {
          FileReader properties = new FileReader("CrystalExtract.properties");   
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
            if (propname.equals("inDir   "))
                inDir = propval;      
            if (propname.equals("outDir  "))
                outDir = propval;            
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
        outputMessage("Crystal extract proccesing started at "+time);
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
            processPDFs();
        time = new java.util.Date().toString().substring(11,19);
        outputMessage("Crystal extract procesing completed at "+time);
        closeLogFile();
    } 
    
    // Process PDFs extracted from Crystal
    // They will be renamed to a standard format and added to a control file
    private void processPDFs()
    {// open control and error files
        openControlFile(); 
        writeToControlFile(CONTROLHEADER);
        openErrorFile();
        int controlCount = 0, errorCount = 0;
        int PDFCount = 0;
        // Process each PDF in the in directory
        File inDirectory = new File(inDir);
        File[] PDFArray = inDirectory.listFiles();
        for (int i = 0; i < PDFArray.length; i++)
        {
            String PDFFilename = PDFArray[i].getName();
            // Extract invoice id and crstal report name from filename
            boolean inProgress = true, idComplete = false;
            int scanPos = 0;
            String id = "";
            long invoiceId = 0;
            while (inProgress)
            {
              String testChar = PDFFilename.substring(scanPos, scanPos+1);
              if (idComplete)
              {
                  if (testChar.startsWith(USCORE))
                      inProgress = false;           // stop processing at second underscore
              }
              else
              {
                  if (testChar.startsWith(USCORE))
                      idComplete = true;
                  else
                      id = id + testChar;           // keep building id string until first underscore
              }
              scanPos++;
            }
            String reportType = PDFFilename.substring(scanPos,PDFFilename.length());
            invoiceId = Long.parseLong(id);
            // Get database data required for control file
            
            if (createOracleObject())
            {
                ResultSet invoiceDetailsRS = oDB.getInvoiceDetails(invoiceId);
                try
                {
                    if(invoiceDetailsRS.next())
                    {
                        String returnBillingSource = invoiceDetailsRS.getString("Billing_Source");
                        String accountNumber = invoiceDetailsRS.getString("Account_Number");
                        String accountName = invoiceDetailsRS.getString("Account_Name");
                        String invoiceNo = invoiceDetailsRS.getString("Invoice_No");
                        String taxPointDate = invoiceDetailsRS.getString("Tax_Point_Date");
                        String periodFromDate = invoiceDetailsRS.getString("Period_From_Date");
                        String periodToDate = invoiceDetailsRS.getString("Period_To_Date");
                        String invoiceTotal = invoiceDetailsRS.getString("Invoice_Total");
                        String currencyCode = invoiceDetailsRS.getString("ISO_Currency_Code");
                       
                        if (invoiceNo.startsWith(NOTFOUND))
                        { 
                            // if invoice not found write to error file
                            writeToErrorFile(PDFFilename+COMMA+INVOICEMISSING);
                            errorCount++;
                        }
                        else if (!returnBillingSource.equals(billingSource))
                        {
                            // if invoice not for current billing source write to error file
                            writeToErrorFile(PDFFilename+COMMA+WRONGBILLINGSOURCE+SPACE+returnBillingSource);
                            errorCount++;
                            
                        }
                        else if (!inDateRange(startDate,endDate,taxPointDate))
                        {
                            // if invoice tax point date not in range write to error file
                            writeToErrorFile(PDFFilename+COMMA+NOTINDATERANGE+SPACE+taxPointDate);
                            errorCount++;                            
                        }
                        else
                        {  
                            // determine file type and file name
                            String fileType = determineFileType(reportType); 
                            String fileName = determineFileName(accountNumber,invoiceNo,fileType,reportType);
                            // rename file into out directory
                            File origFile = new File(inDir+File.separator+PDFFilename);
                            File newFile = new File(outDir+File.separator+fileName);
                            if (origFile.renameTo(newFile))
                            {
                               // write to control file        
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
                                            fileType,
                                            fileName));
                                controlCount++; 
                            }
                            else
                            {
                                // if failed to move renamed file write to error file
                                writeToErrorFile(PDFFilename+COMMA+MOVEFAILED+SPACE+fileName);
                                errorCount++;  
                            }                            
                        }
                    }
                }
                catch(java.sql.SQLException ex)
                {
                    String ioMessage = ex.getMessage();
                    if (!ioMessage.startsWith("Io exception: Connection reset"))
                    {
                        message = 
                        "   DB ERROR: Failed to process invoiceDetailsRS "+
                        ex.getMessage(); 
                        outputMessage(message);
                    }
                }
                invoiceDetailsRS = null;
                PDFCount++;
            }
        }
        destroyOracleObject();
        if (PDFCount==0)
        {
            outputMessage("   ");
            outputMessage("   There are no Crystal PDFs to process");
            outputMessage("   ");
        }
        else
        {
            outputMessage("   ");
            outputMessage("   No. Crystal PDFs processed : "+PDFCount);
            outputMessage("      No. reformatted         : "+controlCount);
            outputMessage("      No. failures            : "+errorCount);
            outputMessage("   ");
            
        }       
        
        // write control and error file footer
        writeToControlFile("Total = "+controlCount);
        writeToErrorFile("Total = "+errorCount);
        
        // close control and error files
        closeControlFile();
        closeErrorFile();  
        
    }
    
    // check that invoice tax point date is in range
    private boolean inDateRange(String sDate, String eDate, String tpdDate)
    {
        boolean result = false;
        SimpleDateFormat sdf = new SimpleDateFormat("dd-MMM-yyyy");
        try
        {
            Date startDate = sdf.parse(sDate);
            Date endDate = sdf.parse(eDate);
            Date taxPointDate = sdf.parse(tpdDate);
            if ((taxPointDate.compareTo(startDate)>=0)&&
                 (taxPointDate.compareTo(endDate)<=0))
                result = true;
        }
        catch (ParseException ex)
        {
            System.out.println("   Error checking date ranges : "+ ex.getMessage());
        }
        return result;
    }
    
    // work out file type for crystal report type
    private String determineFileType(String crystalReportType)
    {
        String fileType = crystalReportType;
        // add in code to determine file type
        if (crystalReportType.startsWith(DATAINVOICERPT))
            fileType = INVOICE;
        else if (crystalReportType.startsWith(GCB1BILLINGREGIONSUMMARYRPT))
            fileType = BILLINGREGIONSUMMARY;
        else if (crystalReportType.startsWith(GCB1SERVICEREFERENCESUMMARYRPT))
            fileType = SERVICEREFERENCESUMMARY;
        else if (crystalReportType.startsWith(GCB1SITEDETAILRPT))
            fileType = SITEDETAIL;
        else if (crystalReportType.startsWith(GCB1SERVICEREFERENCEDETAILRPT))
            fileType = SERVICEREFERENCEDETAIL;
        else if (crystalReportType.startsWith(GCB1TAXSUMMARYRPT))
            fileType = TAXSUMMARY;
        else if (crystalReportType.startsWith(GCB1TAXDETAILRPT))
            fileType = TAXDETAIL;
        else if (crystalReportType.startsWith(GCB1TAXRPT))
            fileType = TAX;
        else if (crystalReportType.startsWith(GCB1SERVICEREFERENCESUMMARYALTRPT))
            fileType = SERVICEREFERENCESUMMARY;
        else if (crystalReportType.startsWith(GCB1SERVICEREFERENCEDETAILALTRPT))
            fileType = SERVICEREFERENCEDETAIL;
        else if (crystalReportType.startsWith(GCB1CPSVOICEUSAGESUMMARYPT))
            fileType = CPSVOICEUSAGESUMMARY;
        else if (crystalReportType.startsWith(STRATEGICINVOICERPT))
            fileType = INVOICE;
        else if (crystalReportType.startsWith(STRATEGICPNN3DETAILRPT))
            fileType = SERVICESUMMARY;
        else if (crystalReportType.startsWith(GCB3INVOICEQUARTERLYRPT))
            fileType = INVOICE + QUARTERLYSUFFIX;
        else if (crystalReportType.startsWith(GCB3INVOICERPT))
            fileType = INVOICE;
        else if (crystalReportType.startsWith(GCB3LEASEDCONSOLSUMMARYBYDRPT))
            fileType = LEASEDCONSOLSUMMARY;
        else if (crystalReportType.startsWith(GCB3LEASEDCONSOLSUMMARYQBMRRPT))
            fileType = LEASEDCONSOLSUMMARY + QUARTERLYSUFFIX;
        else if (crystalReportType.startsWith(GCB3LEASEDCONSOLSUMMARYRPT))
            fileType = LEASEDCONSOLSUMMARY;
        else if (crystalReportType.startsWith(GCB3LEASEDACREDITDJUSTMENTSRPT))
            fileType = LEASEDCREDITANDADJUSTMENTS;
        else if (crystalReportType.startsWith(GCB3LEASEDRENTALSRPT))
            fileType = LEASEDRENTALS;
        else if (crystalReportType.startsWith(GCB3LEASEDRENTALSBYDRPT))
            fileType = LEASEDRENTALS;
        else if (crystalReportType.startsWith(GCB3LEASEDSUNDRYCHARGESRPT))
            fileType = LEASEDSUNDRYCHARGES;
        else if (crystalReportType.startsWith(GCB3LEASEDINSTALLATIONCHARGESRPT))
            fileType = LEASEDINSTALLATIONCHARGES;
        else if (crystalReportType.startsWith(GCB3SWITCHEDCONSOLSUMMARY1QBMRRPT))
            fileType = SWITCHEDCONSOLSUMMARY + QUARTERLYSUFFIX;
        else if (crystalReportType.startsWith(GCB3SWITCHEDCONSOLSUMMARY1RPT))
            fileType = SWITCHEDCONSOLSUMMARY;
        else if (crystalReportType.startsWith(GCB3SWITCHEDCONSOLSUMMARY2QBMRRPT))
            fileType = SWITCHEDCONSOLSUMMARY + QUARTERLYSUFFIX;
        else if (crystalReportType.startsWith(GCB3SWITCHEDCONSOLSUMMARY2WOLRPT))
            fileType = SWITCHEDCONSOLSUMMARY;
        else if (crystalReportType.startsWith(GCB3SWITCHEDCONSOLSUMMARY2BYDRPT))
            fileType = SWITCHEDCONSOLSUMMARY;
        else if (crystalReportType.startsWith(GCB3SWITCHEDCONSOLSUMMARY2RPT))
            fileType = SWITCHEDCONSOLSUMMARY;
        else if (crystalReportType.startsWith(GCB3SWITCHEDEASYACCESSQUARTERLYRPT))
            fileType = SWITCHEDEASYACCESSQUARTERLY;
        else if (crystalReportType.startsWith(GCB3SWITCHEDEASYACCESSUSAGERPT))
            fileType = SWITCHEDEASYACCESSUSAGE;
        else if (crystalReportType.startsWith(GCB3SWITCHEDEASYACCESSUSAGERPT))
            fileType = SWITCHEDEASYACCESSUSAGE;
        else if (crystalReportType.startsWith(GCB3SWITCHEDINSTALLATIONCHARGESRPT))
            fileType = SWITCHEDINSTALLATIONCHARGES;
        else if (crystalReportType.startsWith(GCB3SWITCHEDINSTALLATIONCHARGESBYDRPT))
            fileType = SWITCHEDINSTALLATIONCHARGES;
        else if (crystalReportType.startsWith(GCB3SWITCHEDRENTALCHARGESRPT))
            fileType = SWITCHEDRENTALCHARGES;
        else if (crystalReportType.startsWith(GCB3SWITCHEDRENTALCHARGESBYDRPT))
            fileType = SWITCHEDRENTALCHARGES;
        else if (crystalReportType.startsWith(GCB3SWITCHEDSOURCEDISCOUNTSRPT))
            fileType = SWITCHEDSOURCEDISCOUNTS;
        else if (crystalReportType.startsWith(GCB3SWITCHEDSOURCEDISCOUNTSBYDRPT))
            fileType = SWITCHEDSOURCEDISCOUNTS;
        else if (crystalReportType.startsWith(GCB3SWITCHEDSPECIALCHARGESRPT))
            fileType = SWITCHEDSPECIALCHARGES;
        else if (crystalReportType.startsWith(GCB3SWITCHEDSUNDRYCHARGESRPT))
            fileType = SWITCHEDSUNDRYCHARGES;
        else if (crystalReportType.startsWith(GCB3SWITCHEDSUNDRYCHARGESBYDRPT))
            fileType = SWITCHEDSUNDRYCHARGES;
        else if (crystalReportType.startsWith(GCB3SWITCHEDUSAGESUMMARY18RPT))
            fileType = SWITCHEDUSAGESUMMARY;
        else if (crystalReportType.startsWith(GCB3SWITCHEDUSAGESUMMARY19RPT))
            fileType = SWITCHEDUSAGESUMMARY;
        else if (crystalReportType.startsWith(GCB3SWITCHEDVPNCHARGESRPT))
            fileType = SWITCHEDVPNCHARGES;
        else if (crystalReportType.startsWith(GCB3SWITCHEDAUTHCODECHARGESRPT))
            fileType = SWITCHEDAUTHCODECHARGES;
        else if (crystalReportType.startsWith(GCB3SWITCHEDBILLINGNUMBERCHARGES4RPT))
            fileType = SWITCHEDBILLINGNUMBERCHARGES;
        else if (crystalReportType.startsWith(GCB3SWITCHEDBILLINGNUMBERCHARGES4BYDRPT))
            fileType = SWITCHEDBILLINGNUMBERCHARGES;
        else if (crystalReportType.startsWith(GCB3SWITCHEDBILLINGNUMBERCHARGES5RPT))
            fileType = SWITCHEDBILLINGNUMBERCHARGES;
        else if (crystalReportType.startsWith(GCB3SWITCHEDBILLINGNUMBERCHARGES52RPT))
            fileType = SWITCHEDBILLINGNUMBERCHARGES;
        else if (crystalReportType.startsWith(GCB3SWITCHEDBILLINGNUMBERCHARGES4WOLRPT))
            fileType = SWITCHEDBILLINGNUMBERCHARGES;
        else if (crystalReportType.startsWith(GCB3SWITCHEDCALLCHARGESSUMMARYRPT))
            fileType = SWITCHEDCALLCHARGESSUMMARY;
        else if (crystalReportType.startsWith(GCB3SWITCHEDCALLCHARGESSUMMARYBYDRPT))
            fileType = SWITCHEDCALLCHARGESSUMMARY;
        else if (crystalReportType.startsWith(GCB3SWITCHEDCALLINKCHARGESRPT))
            fileType = SWITCHEDCALLINKCHARGES;
        else if (crystalReportType.startsWith(GCB3SWITCHEDCALLINKCHARGESBYDRPT))
            fileType = SWITCHEDCALLINKCHARGES;
        else if (crystalReportType.startsWith(GCB3SWITCHEDACREDITDJUSTMENTSRPT))
            fileType = SWITCHEDCREDITANDADJUSTMENTS;        
        else if (crystalReportType.startsWith(GCB5INVOICERPT))
            fileType = INVOICE;       
        else if (crystalReportType.startsWith(GCB5INVOICEBELRPT))
            fileType = INVOICE;
        else if (crystalReportType.startsWith(MANAGEDSERVICESINVOICERPT))
            fileType = INVOICE;
        else if (crystalReportType.startsWith(GCB7STRATEGICGLOBALDIALDETAIL))
            fileType = SERVICESUMMARY;
        else if (crystalReportType.startsWith(GSOINVOICERPT))
            fileType = INVOICE;
        else if (crystalReportType.startsWith(GSOSUMMARYSHEETRPT))
            fileType = SUMMARYSHEET;
        else if (crystalReportType.startsWith(GSODETAILEDBILLINGRPT))
            fileType = DETAILEDBILLING;
        else if (crystalReportType.startsWith(MMSINVOICERPT))
            fileType = INVOICE;
        else if (crystalReportType.startsWith(SYBASEINVOICERPT))
            fileType = INVOICE;
        else if (crystalReportType.startsWith(MMSCHARGESSUMNMARYRPT))
            fileType = CHARGESSUMMARY;
        else if (crystalReportType.startsWith(SYBASECHARGESSUMNMARYRPT))
            fileType = CHARGESSUMMARY;
        else if (crystalReportType.startsWith(MMSTERMINATIONSUMNMARYRPT))
            fileType = TERMINATIONSUMMARY;
        else if (crystalReportType.startsWith(SYBASETERMINATIONSUMNMARYRPT))
            fileType = TERMINATIONSUMMARY;
        else if (crystalReportType.startsWith(MMSTRANSITSUMNMARYRPT))
            fileType = TRANSITSUMMARY;
        else if (crystalReportType.startsWith(SYBASETRANSITSUMNMARYRPT))
            fileType = TRANSITSUMMARY;
        else if (crystalReportType.startsWith(NOSTROINVOICERPT))
            fileType = INVOICE;
        else if (crystalReportType.startsWith(NOSTROSUMMARYRPT))
            fileType = SUMMARY;
        else if (crystalReportType.startsWith(NOSTRODETAILRPT))
            fileType = DETAIL;
        else if (crystalReportType.startsWith(TBSUMMARYPT))
            fileType = PREMIUMRATESUMMARY;
        else if (crystalReportType.startsWith(TBCRSUMMARYPT))
            fileType = PREMIUMRATECALLSUMMARY;
        else if (crystalReportType.startsWith(TBCRDETAILRPT))
            fileType = PREMIUMRATECALLDETAIL;
        return fileType;
    }
    
    // work out filename for filetype
    private String determineFileName(String accountNumber, String invoiceNo, String fileType, String crystalReportType)
    {
        String filename = accountNumber+USCORE+invoiceNo.replace(FWDSLASH, HYPHEN)+USCORE;
        if (fileType.endsWith(QUARTERLYSUFFIX))
            filename = 
                filename + 
                fileType.substring(0,fileType.length()-12).replaceAll(SPACE, USCORE) + 
                PDFEXT;
        else if (crystalReportType.startsWith(DATAINVOICERPT))
            filename = filename + DATAINVOICE.replaceAll(SPACE, USCORE) + PDFEXT;
        else if (crystalReportType.startsWith(STRATEGICINVOICERPT))
            filename = filename + STRATEGICINVOICE.replaceAll(SPACE, USCORE) + PDFEXT;
        else if ((crystalReportType.startsWith(MANAGEDSERVICESINVOICERPT))||(crystalReportType.startsWith(GCB5INVOICERPT)))
            filename = filename + MANAGEDSERVICESINVOICE.replaceAll(SPACE, USCORE) + PDFEXT;
        else
            filename = filename + fileType.replaceAll(SPACE, USCORE) + PDFEXT;        
        return filename;
    }
    
    // format control file line for report
    private String formatControlFileLine
    (   String accountNumber,
        String accountName,
        String invoiceNumber,
        String taxPointDate,
        String periodFromDate,
        String periodToDate,
        String invoiceTotal,
        String currencyCode,
        String filetype,
        String filename )
    {
        String line = 
            bSource+ COMMA +   
            getAMLBillingSource(bSource) + COMMA +    
            accountNumber + COMMA +
            accountName + COMMA +
            invoiceNumber + COMMA +
            taxPointDate + COMMA +
            periodFromDate + COMMA +
            periodToDate + COMMA +
            invoiceTotal + COMMA +
            currencyCode + COMMA +
            "" + COMMA +
            filetype + COMMA +
            filename;
        return line;
    }     
    
    private String getAMLBillingSource(String billingSource)
    {
        String AMLBillingSource = billingSource;
        // Amend GCB1, GCB3, GCB5, GCB7, MMS and Nostro to GCBR
        if ((billingSource.startsWith(GCB1OUT))||
            (billingSource.startsWith(GCB3OUT))||
            (billingSource.startsWith(GCB5OUT))||
            (billingSource.startsWith(GCB7OUT))||
            (billingSource.startsWith(MMS))||
            (billingSource.startsWith(NOSTRO)))
            AMLBillingSource = GCBR;
        // Amend GSO to GCBW
        if (billingSource.startsWith(GSO))
            AMLBillingSource = GCBW;
        // Amend TeleBusiness to SSBS
        if (billingSource.startsWith(TB))
            AMLBillingSource = SSBS;
        return AMLBillingSource;
    }
    
    // output message to log file and command line
    private void outputMessage(String inMessage)
    {
        System.out.println(inMessage.trim());
        writeToLogFile(inMessage);
    }
    
    // validate billing source
    private boolean validBillingSource(String inBillSrc)
    {
        boolean result = false;
        // check that supplied billing source is one of the expected values
        if ( (inBillSrc.equals(GCB1))  ||
             (inBillSrc.equals(GCB3))  ||
             (inBillSrc.equals(GCB5))  ||
             (inBillSrc.equals(GCB7))  ||
             (inBillSrc.equals(GSO))   ||
             (inBillSrc.equals(MMS))   ||
             (inBillSrc.equals(TB))    ||
             (inBillSrc.equals(NOSTRO))  )
            result = true;
        return result;
    }
    
    // open log file
    private void openLogFile()
    {
        logBW = null;
        String logDate = new java.util.Date().toString();
        String logFilename = 
            logDir+File.separator+"crystalExtract_"+
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
    // clears OracleDB object
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

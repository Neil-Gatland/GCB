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

public class processInitialReportRequests 
{
    // class variables
    private String dbServer, username, password;
    private String logDir, batchDir, xmlDir, xslDir, pdfTrialDir, pdfClosedDir;
    private String message;
    private BufferedWriter logBW, scriptBW;
    private Connection conn;
    private sqlServerDB sDB;
    private boolean dbOK;
    // class constants    
    // Database values
    private final String GIVNREF = "givn_ref";
    //private final String GCD = "GCD";
    //private final String CONGLOMERATE = "Conglomerate";
    //private final String DIALUPPRODUCTS = "Dialup_Products";
    // Billing Source values
    private final String GLOBALDIAL = "Globaldial";
    private final String CONGLOM = "Conglom";
    // Batch script file constants
    private final String SCRIPTCOMMENT = "::";
    private final String ECHO1START = "echo :XML ON>";
    private final String ECHO2START = "echo";
    private final String ECHO2MIDDLEREPORT = ">>";
    private final String ECHO2MIDDLEEXTRACT = ">";
    private final String ECHO3 = "echo GO>>";
    private final String SQLCMDSTART = "sqlcmd -S ";
    /*private final String SQLCMDMIDDLE1REPORT = " -E -i "; 
    private final String SQLCMDMIDDLE1EXTRACT = " -E -W -s , -h -1 -i ";*/
    private final String SQLCMDMIDDLE1REPORT = " -i "; 
    private final String SQLCMDMIDDLE1EXTRACT = " -W -s , -h -1 -i "; 
    private final String SQLCMDMIDDLE2 = " -o "; 
    private final String FOPSTART = "fop -xml ";
    private final String FOPMIDDLE1 = " -xsl "; 
    private final String FOPMIDDLE2 = " -pdf ";  
    private final String ZIPSTART = "zip -j ";  
    private final String ZIPMIDDLE = " ";
    private final String EXTRACT = "Extract";
    // String constant values
    private final String SINGLESPACE = " ";
    private final String COMMA = ",";
    private final String SINGLEQUOTE = "'";
    private final String USCORE = "_";
    // File extensions
    private final String BATEXT = ".bat";
    private final String XMLEXT = ".xml";
    private final String CSVEXT = ".csv";
    private final String LOGEXT = "_log.txt";
    // File name constants
    private final String WORKSQLFILENAME = "work.sql";
    // Report/Extract names
    private final String GCB1INV = "gcb1Invoice";
    private final String GCB1DET = "gcb1Detail";
    private final String GCB5INV = "gcb5Invoice";
    private final String STRATEGICINV = "strategicInvoice";
    private final String STRATEGICDET = "strategicDetail";
    private final String ADHOCDATAEXTRACT = "AdhocDataExtract";
    private final String GCB3INVOICE = "gcb3Invoice";
    private final String GCB3CONSOLIDATED = "gcb3Consolidated";
    private final String GCB3USAGE = "gcb3Usage";
    private final String GCB3ADJUSTMENTS = "gcb3Adjustments";
    private final String GCB3RENTALS = "gcb3Rentals";
    private final String GCB3INSTALLS = "gcb3Installs";
    private final String GCB3AUTHCODECHARGES = "gcb3AuthcodeCharges";
    private final String GCB3SUNDRY = "gcb3Sundry";
    private final String GCB3BILLINGNUMBERCHARGES = "gcb3BillingNumberCharges";
    private final String GCB3CALLCHARGES = "gcb3CallCharges";
    private final String GCB3CALLINK = "gcb3Callink";
    private final String GCB3EASYQUARTERLY = "gcb3EasyQuarterly";
    private final String GCB3EASYUSAGECHARGES = "gcb3EasyUsageCharges";
    private final String GCB3SOURCEDISCOUNTS = "gcb3SourceDiscounts";
    private final String GCB3SPECIALCHARGES = "gcb3SpecialCharges";
    private final String GCB3VPNCHARGES = "gcb3VPNCharges";
    private final String CONGLOMINVOICEDOWNLOAD = "conglomInvoiceDownload";
    private final String CHARGESUMMARYDOWNLOAD = "chargeSummaryDownload";
    private final String CONGLOMBILLINGNUMBERSUMMARY = "conglomBillingNumberSummary";
    private final String SAVILLSIMPORT = "savillsImport";
    private final String BILLINGNUMBERCHARGES = "billingNumberCharges";
    private final String COLTRENTALS = "coltRentals";
    private final String COLTINSTALLS = "coltInstalls";
    private final String TALISMANINBOUND = "talismanInbound";
    private final String TALISMANOUTBOUND = "talismanOutbound";
    private final String BILLINGNUMBER = "BillingNumber";
    private final String CALLCHARGESSUMMARY = "CallChargesSummary";
    private final String CALLINKCHARGES = "CallinkCharges";
    private final String CONSOLIDATEDSUMMARY = "ConsolidatedSummary";
    private final String EASYACCESSUSAGECHARGES = "EasyAccessUsageCharges";
    private final String INVOICE = "Invoice";
    private final String RENTALCHARGES = "RentalCharges";
    private final String USAGESUMMARY = "UsageSummary";
    private final String VPNCHARGESUMMARY = "VPNChargeSummary";
    private final String INSTALLCHARGES = "InstallCharges";
    private final String SOURCEDISCOUNTS = "SourceDiscounts";
    private final String SPECIALCHARGES ="SpecialCharges";
    private final String SUNDRYCHARGES ="SundryCharges";
    private final String CREDITSANDADJUSTMENTS = "CreditsAndAdjustments";
    // Stored procedure execs for XML production
    private final String GCB1INVEXEC = "exec gcd..FOP_Get_GCB1_Invoice_XML"; 
    private final String GCB1DETEXEC = "exec gcd..FOP_Get_GCB1_Detail_XML"; 
    private final String GCB5INVEXEC = "exec gcd..FOP_Get_GCB5_Invoice_XML";
    private final String STRATEGICINVEXEC = "exec gcd..FOP_Get_Strategic_Invoice_XML"; 
    private final String STRATEGICDETEXEC = "exec gcd..FOP_Get_Strategic_Detail_XML"; 
    private final String STRATEGICDETRAMEXEC = "exec Dialup_Products..FOP_Get_Strategic_Detail_XML";  
    private final String ADHOCDATAEXTRACTEXEC = "exec gcd..Get_Adhoc_Data_Invoice_Extract";
    private final String GCB3INVOICEEXEC = "exec Conglomerate..FOP_Get_GCB3_Invoice_XML";
    private final String GCB3CONSOLIDATEDEXEC = "exec Conglomerate..FOP_Get_GCB3_Consolidated_XML";
    private final String GCB3USAGEEXEC = "exec Conglomerate..FOP_Get_GCB3_Usage_Summary_XML";
    private final String GCB3ADJUSTMENTSEXEC = "exec Conglomerate..FOP_Get_GCB3_Adjustments_XML";
    private final String GCB3RENTALSEXEC = "exec Conglomerate..FOP_Get_GCB3_Rentals_XML";
    private final String GCB3INSTALLSEXEC = "exec Conglomerate..FOP_Get_GCB3_Installs_XML";
    private final String GCB3AUTHCODECHARGESEXEC = "exec Conglomerate..FOP_Get_GCB3_AuthCode_XML";
    private final String GCB3SUNDRYEXEC = "exec Conglomerate..FOP_Get_GCB3_Sundry_XML";
    private final String GCB3BILLINGNUMBERCHARGESEXEC = "exec Conglomerate..FOP_Get_GCB3_Billing_No_Summary_XML";
    private final String GCB3CALLCHARGESEXEC = "exec Conglomerate..FOP_Get_GCB3_Call_Charges_Summary_XML";
    private final String GCB3CALLINKEXEC = "exec Conglomerate..FOP_Get_GCB3_Callink_Charges_XML";
    private final String GCB3EASYQUARTERLYEXEC = "exec Conglomerate..FOP_Get_GCB3_Easy_Access_Quarterly_Charges_XML";
    private final String GCB3EASYUSAGECHARGESEXEC = "exec Conglomerate..FOP_Get_GCB3_Easy_Usage_Charges_XML";
    private final String GCB3SOURCEDISCOUNTSEXEC = "exec Conglomerate..FOP_Get_GCB3_Source_Discount_XML";
    private final String GCB3SPECIALCHARGESEXEC = "exec Conglomerate..FOP_Get_GCB3_Special_Charges_XML";
    private final String GCB3VPNCHARGESEXEC = "exec Conglomerate..FOP_Get_GCB3_VPN_Charges_XML";
    private final String CONGLOMINVOICEDOWNLOADEXEC = "exec Conglomerate..Get_Conglom_Invoice_Download";
    private final String CHARGESUMMARYDOWNLOADEXEC = "exec Conglomerate..Get_Charge_Summary_Download";
    private final String CONGLOMBILLINGNUMBERSUMMARYEXEC = "exec Conglomerate..Get_Conglom_Billing_Number_Summary";
    private final String SAVILLSIMPORTEXEC = "exec Conglomerate..Get_Savills_Import";
    private final String BILLINGNUMBERCHARGESEXEC = "exec Conglomerate..Get_Billing_Number_Charges";
    private final String COLTRENTALSEXEC = "exec Conglomerate..Get_Colt_Rentals";
    private final String COLTINSTALLSEXEC = "exec Conglomerate..Get_Colt_Installs";
    private final String TALISMANINBOUNDEXEC = "exec Conglomerate..Get_Talisman_Inbound";
    private final String TALISMANOUTBOUNDEXEC = "exec Conglomerate..Get_Talisman_Outbound";
    private final String BILLINGNUMBEREXEC = "exec Conglomerate..Get_Billing_Number";
    private final String CALLCHARGESSUMMARYEXEC = "exec Conglomerate..Get_Call_Charges_Summary";
    private final String CALLINKCHARGESEXEC = "exec Conglomerate..Get_Callink_Charges";
    private final String CONSOLIDATEDSUMMARYEXEC = "exec Conglomerate..Get_Consolidated_Summary";
    private final String EASYACCESSUSAGECHARGESEXEC = "exec Conglomerate..Get_Easy_Access_Usage_Charges";
    private final String INVOICEEXEC = "exec Conglomerate..Get_Invoice";
    private final String RENTALCHARGESEXEC = "exec Conglomerate..Get_Rental_Charges";
    private final String USAGESUMMARYEXEC = "exec Conglomerate..Get_Usage_Summary";
    private final String VPNCHARGESUMMARYEXEC= "exec Conglomerate..Get_VPN_Charge_Summary";
    private final String INSTALLCHARGESEXEC= "exec Conglomerate..Get_Install_Charges";
    private final String SOURCEDISCOUNTSEXEC= "exec Conglomerate..Get_Source_Discounts";
    private final String SPECIALCHARGESEXEC= "exec Conglomerate..Get_Special_Charges";
    private final String SUNDRYCHARGESEXEC= "exec Conglomerate..Get_Sundry_Charges";
    private final String CREDITSANDADJUSTMENTSEXEC= "exec Conglomerate..Get_Credits_And_Adjustments";
    // Stylesheet names
    private final String GCB1INVSTYLESHEET = "gcb1Invoice.xsl"; 
    private final String GCB1DETSTYLESHEET = "gcb1Detail.xsl"; 
    private final String GCB5INVSTYLESHEET = "gcb5Invoice.xsl";
    private final String STRATEGICINVSTYLESHEET = "strategicInvoice.xsl"; 
    private final String STRATEGICDETSTYLESHEET = "strategicDetail.xsl";
    private final String GCB3INVSTYLESHEET = "gcb3Invoice.xsl";
    private final String GCB3CONSOLIDATEDSTYLESHEET = "gcb3Consolidated.xsl";
    private final String GCB3USAGESTYLESHEET = "gcb3UsageSummary.xsl";
    private final String GCB3ADJUSTMENTSSTYLESHEET = "gcb3Adjustments.xsl";
    private final String GCB3RENTALSSTYLESHEET = "gcb3Rentals.xsl";
    private final String GCB3INSTALSSTYLESHEET = "gcb3Installs.xsl";
    private final String GCB3AUTHCODESTYLESHEET = "gcb3Authcode.xsl";
    private final String GCB3SUNDRYSTYLESHEET = "gcb3Sundry.xsl";
    private final String GCB3BILLEDNOSUMMARYSTYLESHEET = "gcb3BilledNoSummary.xsl";
    private final String GCB3CALLCHARGESSTYLESHEET = "gcb3CallChargesSummary.xsl";
    private final String GCB3CALLINKCHARGESSTYLESHEET = "gcb3CallinkCharges.xsl";
    private final String GCB3EASYACCESSQUARTERLYCHARGESSTYLESHEET = "gcb3EasyAccessQuarterlyCharges.xsl";
    private final String GCB3EASYUSAGECHARGESSTYLESHEET = "gcb3EasyUsageCharges.xsl";
    private final String GCB3SOURCEDISCOUNTSTYLESHEET = "gcb3SourceDiscount.xsl";
    private final String GCB3SPECIALCHARGESSTYLESHEET = "gcb3SpecialCharges.xsl";
    private final String GCB3VPNCHARGESSTYLESHEET = "gcb3VPNCharges.xsl";
    // Progress message
    private final String SUCCESSMESSAGE = " : OK";
    private final String FAILUREMESSAGE = " : FAILED";
    private final String EMPTYMESSAGE = " No outstanding requests to process";
    
    public static void main(String[] args)
    {
        // control processing
        processInitialReportRequests pIRR = new processInitialReportRequests();
        pIRR.control();
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
            if (propname.equals("batchdir"))
              batchDir = propval;
            if (propname.equals("xmldir  "))
              xmlDir = propval;
            if (propname.equals("xsldir  "))
              xslDir = propval;
            if (propname.equals("pdftdir "))
              pdfTrialDir = propval;
            if (propname.equals("pdfcdir "))
              pdfClosedDir = propval;
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
        // process report requests that are status Requested
        if (dbOK)
        {
            boolean noRequests = true;
            int successCount = 0, failureCount = 0;
            String updateINString = "(", scriptLine = "";
            String workSQLFilename = batchDir + File.separator + WORKSQLFILENAME;
            String workXMLFilename = "";
            ResultSet requestsRS = sDB.getRequestedReportRequests();
            try
            {
                while((requestsRS.next())&&(dbOK))
                {
                    boolean scriptingOK = true, report = true;
                    if (noRequests)
                        noRequests = false;
                    // get relevant data from current report request
                    long reportRequestId = requestsRS.getInt("Report_Request_Id");
                    String globalCustomerId = requestsRS.getString("Global_Customer_Id").trim();
                    String invoiceNo = requestsRS.getString("Invoice_No").trim();
                    String trialInd = requestsRS.getString("Trial_Ind").trim();
                    String reportName = requestsRS.getString("Report_Name").trim();
                    String reportType = requestsRS.getString("Report_Type").trim();
                    if (reportType.startsWith(EXTRACT))
                        report = false;
                    String reportFilename = requestsRS.getString("Report_Filename").trim();
                    String billingSource = requestsRS.getString("Billing_Source").trim();
                    String fileInvoiceNo = fileInvoiceNo(invoiceNo,billingSource);
                    String billedProductId = requestsRS.getString("Billed_Product_Id");
                    String ldFlag = requestsRS.getString("LD_Flag");
                    String requestMessage = "   " +
                        globalCustomerId + COMMA + SINGLESPACE + 
                        invoiceNo + COMMA + SINGLESPACE +
                        reportName + SINGLESPACE; 
                    // open batch script file for report request
                    if (!openScriptFile(globalCustomerId,fileInvoiceNo,reportName,billedProductId))
                        scriptingOK = false;
                    // add header comment to script file
                    if (scriptingOK)
                    {                        
                        scriptLine =
                            SCRIPTCOMMENT + SINGLESPACE +
                            globalCustomerId + COMMA +
                            invoiceNo + COMMA +
                            reportName + COMMA +
                            trialInd;
                        if (!writeToScriptFile(scriptLine))
                            scriptingOK = false;                                     
                    }
                    // first echo line 
                    if ((scriptingOK)&&(report))  // ignore for extracts
                    {   
                        scriptLine = ECHO1START + workSQLFilename;
                        if (!writeToScriptFile(scriptLine))
                            scriptingOK = false;                                     
                    }
                    // second echo line
                    if (scriptingOK)
                    { 
                        scriptLine = 
                            ECHO2START + SINGLESPACE + 
                            determineExecSQL(reportName,billingSource) + SINGLESPACE +
                            determineParameters(
                                reportName,
                                report,
                                billingSource,
                                globalCustomerId,
                                invoiceNo,
                                billedProductId,
                                ldFlag);
                        if (report)
                            scriptLine = scriptLine + ECHO2MIDDLEREPORT + workSQLFilename;
                        else
                            scriptLine = scriptLine + ECHO2MIDDLEEXTRACT + workSQLFilename;
                        if (!writeToScriptFile(scriptLine))
                            scriptingOK = false;                                     
                    }
                    // third echo line
                    if (scriptingOK)
                    {       
                        scriptLine = ECHO3 + workSQLFilename;
                        if (!writeToScriptFile(scriptLine))
                            scriptingOK = false;                                     
                    }
                    // sqlcmd line
                    if (scriptingOK)
                    {       
                        
                        if (report)
                        {   
                            workXMLFilename = 
                                xmlDir + File.separator + 
                                globalCustomerId + USCORE + 
                                fileInvoiceNo + USCORE +
                                reportName + XMLEXT;
                            scriptLine = 
                                SQLCMDSTART + dbServer + 
                                " -U " + username + " -P " + password +    
                                SQLCMDMIDDLE1REPORT + workSQLFilename +
                                SQLCMDMIDDLE2 + workXMLFilename;                            
                        }                            
                        else
                        {                              
                            workXMLFilename = 
                                xmlDir + File.separator + 
                                globalCustomerId + USCORE + 
                                fileInvoiceNo + USCORE +
                                reportName + CSVEXT;   
                            scriptLine = 
                                SQLCMDSTART + dbServer +
                                " -U " + username + " -P " + password + 
                                SQLCMDMIDDLE1EXTRACT + workSQLFilename +
                                SQLCMDMIDDLE2 + workXMLFilename;
                        }
                        if (!writeToScriptFile(scriptLine))
                            scriptingOK = false;                                     
                    }
                    // fop/zip line line 
                    if (scriptingOK)
                    {     
                        if (report)
                        {
                            String pdfFilename = "";
                            if (trialInd.startsWith("Y"))
                                pdfFilename = pdfTrialDir + File.separator + reportFilename;
                            else
                                pdfFilename = pdfClosedDir + File.separator + reportFilename;
                            scriptLine = 
                                FOPSTART + workXMLFilename +
                                FOPMIDDLE1 + xslDir + File.separator + determineStylesheet(reportName) +
                                FOPMIDDLE2 + pdfFilename;
                        }
                        else
                        {
                            String zipFileName = pdfClosedDir + File.separator + reportFilename;
                            scriptLine = 
                                ZIPSTART + zipFileName +
                                ZIPMIDDLE + workXMLFilename;
                                    
                        }
                        if (!writeToScriptFile(scriptLine))
                            scriptingOK = false;                                     
                    }
                    if (scriptingOK)
                    {
                       // Store request id for later setting status to Processed
                        updateINString = updateINString + reportRequestId + COMMA;  
                        requestMessage = requestMessage + SUCCESSMESSAGE;
                        successCount++;
                    }
                    else
                    {
                        requestMessage = requestMessage + FAILUREMESSAGE;  
                        failureCount++;
                    }                                            
                    // success / failure message
                    System.out.println(requestMessage);
                    writeToLogFile(requestMessage);
                    // close batch script file
                    if (!closeScriptFile())
                        scriptingOK = false;
                }
            }
            catch(java.sql.SQLException ex)
            {
                message = "   SQL DB ERROR: Processing requested report requests : " + ex.getMessage();
                System.out.println(message);
                writeToLogFile(message);
                dbOK = false;
            }
            requestsRS = null;
            if (noRequests)
            {
                // no requests to process
                message = "   " + EMPTYMESSAGE;                
                System.out.println(message);
                writeToLogFile(message);
            }
            else
            {
                // summary message
                int totalCount = successCount + failureCount;
                message = "   " + totalCount + " requests processed : " + successCount + " OK : " + failureCount + " FAILED";                
                System.out.println(message);
                writeToLogFile(message);
                // remove ending comma from work IN statement and add close bracket
                updateINString = updateINString.substring(0, updateINString.length() -1 );
                updateINString = updateINString + ")";
                // update all successfully processed report requests to In Progress status
                if (!sDB.updateInProgressReportRequests(updateINString))
                {
                    message = "   SQL DB ERROR: Failed to update successful report requests to In Progress : " + updateINString;
                    System.out.println(message);
                    writeToLogFile(message);
                }
            }
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
    
    private String determineExecSQL(String reportName, String billingSource)
    {
        String result = "exec XXXX";
        if (reportName.startsWith(GCB1INV))
            result = GCB1INVEXEC;
        else if (reportName.startsWith(GCB1DET))
            result = GCB1DETEXEC;
        else if (reportName.startsWith(GCB5INV))
            result = GCB5INVEXEC;
        else if (reportName.startsWith(STRATEGICINV))
            result = STRATEGICINVEXEC;
        else if ((reportName.startsWith(STRATEGICDET)&&(billingSource.startsWith(GLOBALDIAL))))
            result = STRATEGICDETRAMEXEC;
        else if (reportName.startsWith(STRATEGICDET))
            result = STRATEGICDETEXEC;
        else if (reportName.startsWith(ADHOCDATAEXTRACT))
            result = ADHOCDATAEXTRACTEXEC;
        else if (reportName.startsWith(GCB3INVOICE))
            result = GCB3INVOICEEXEC;
        else if (reportName.startsWith(GCB3CONSOLIDATED))
            result = GCB3CONSOLIDATEDEXEC;
        else if (reportName.startsWith(GCB3USAGE))
            result = GCB3USAGEEXEC;
        else if (reportName.startsWith(GCB3ADJUSTMENTS))
            result = GCB3ADJUSTMENTSEXEC;
        else if (reportName.startsWith(GCB3RENTALS))
            result = GCB3RENTALSEXEC;
        else if (reportName.startsWith(GCB3INSTALLS))
            result = GCB3INSTALLSEXEC;
        else if (reportName.startsWith(GCB3AUTHCODECHARGES))
            result = GCB3AUTHCODECHARGESEXEC;
        else if (reportName.startsWith(GCB3SUNDRY))
            result = GCB3SUNDRYEXEC;
        else if (reportName.startsWith(GCB3BILLINGNUMBERCHARGES))
            result = GCB3BILLINGNUMBERCHARGESEXEC;
        else if (reportName.startsWith(GCB3CALLCHARGES))
            result = GCB3CALLCHARGESEXEC;
        else if (reportName.startsWith(GCB3CALLINK))
            result = GCB3CALLINKEXEC;
        else if (reportName.startsWith(GCB3EASYQUARTERLY))
            result = GCB3EASYQUARTERLYEXEC;
        else if (reportName.startsWith(GCB3EASYUSAGECHARGES))
            result = GCB3EASYUSAGECHARGESEXEC;
        else if (reportName.startsWith(GCB3SPECIALCHARGES))
            result = GCB3SPECIALCHARGESEXEC;        
        else if (reportName.startsWith(GCB3SOURCEDISCOUNTS))
            result = GCB3SOURCEDISCOUNTSEXEC;
        else if (reportName.startsWith(GCB3VPNCHARGES))
            result = GCB3VPNCHARGESEXEC;
        else if (reportName.startsWith(CONGLOMINVOICEDOWNLOAD))
            result = CONGLOMINVOICEDOWNLOADEXEC;
        else if (reportName.startsWith(CHARGESUMMARYDOWNLOAD))
            result = CHARGESUMMARYDOWNLOADEXEC;        
        else if (reportName.startsWith(CONGLOMBILLINGNUMBERSUMMARY))
            result = CONGLOMBILLINGNUMBERSUMMARYEXEC;
        else if (reportName.startsWith(SAVILLSIMPORT))
            result = SAVILLSIMPORTEXEC;
        else if (reportName.startsWith(BILLINGNUMBERCHARGES))
            result = BILLINGNUMBERCHARGESEXEC;
        else if (reportName.startsWith(COLTRENTALS))
            result = COLTRENTALSEXEC;
        else if (reportName.startsWith(COLTINSTALLS))
            result = COLTINSTALLSEXEC;
        else if (reportName.startsWith(TALISMANINBOUND))
            result = TALISMANINBOUNDEXEC;
        else if (reportName.startsWith(TALISMANOUTBOUND))
            result = TALISMANOUTBOUNDEXEC;
        else if (reportName.startsWith(BILLINGNUMBER))
            result = BILLINGNUMBEREXEC;
        else if (reportName.startsWith(CALLCHARGESSUMMARY))
            result = CALLCHARGESSUMMARYEXEC;
        else if (reportName.startsWith(CALLINKCHARGES))
            result =  CALLINKCHARGESEXEC;
        else if (reportName.startsWith(CONSOLIDATEDSUMMARY))
            result =  CONSOLIDATEDSUMMARYEXEC;
        else if (reportName.startsWith(EASYACCESSUSAGECHARGES))
            result =  EASYACCESSUSAGECHARGESEXEC;
        else if (reportName.startsWith(INVOICE))
            result =  INVOICEEXEC;
        else if (reportName.startsWith(RENTALCHARGES))
            result =  RENTALCHARGESEXEC;
        else if (reportName.startsWith(USAGESUMMARY))
            result =  USAGESUMMARYEXEC;  
        else if (reportName.startsWith(VPNCHARGESUMMARY))
            result =  VPNCHARGESUMMARYEXEC; 
        else if (reportName.startsWith(INSTALLCHARGES))
            result =  INSTALLCHARGESEXEC; 
        else if (reportName.startsWith(SOURCEDISCOUNTS))
            result =  SOURCEDISCOUNTSEXEC;
        else if (reportName.startsWith(SPECIALCHARGES))
            result =  SPECIALCHARGESEXEC; 
        else if (reportName.startsWith(SUNDRYCHARGES))
            result =  SUNDRYCHARGESEXEC;  
        else if (reportName.startsWith(CREDITSANDADJUSTMENTS))
            result =  CREDITSANDADJUSTMENTSEXEC; 
        return result;
    } 
    
    private String determineStylesheet(String reportName)
    {
        String result = "XXXX.xsl";
        if (reportName.startsWith(GCB1INV))
            result = GCB1INVSTYLESHEET;
        else if (reportName.startsWith(GCB1DET))
            result = GCB1DETSTYLESHEET;
        else if (reportName.startsWith(GCB5INV))
            result = GCB5INVSTYLESHEET;
        else if (reportName.startsWith(STRATEGICINV))
            result = STRATEGICINVSTYLESHEET;
        else if (reportName.startsWith(STRATEGICDET))
            result = STRATEGICDETSTYLESHEET;
        else if (reportName.startsWith(GCB3INVOICE))
            result = GCB3INVSTYLESHEET;
        else if (reportName.startsWith(GCB3CONSOLIDATED))
            result = GCB3CONSOLIDATEDSTYLESHEET;
        else if (reportName.startsWith(GCB3USAGE))
            result = GCB3USAGESTYLESHEET;
        else if (reportName.startsWith(GCB3ADJUSTMENTS))
            result = GCB3ADJUSTMENTSSTYLESHEET;
        else if (reportName.startsWith(GCB3RENTALS))
            result = GCB3RENTALSSTYLESHEET;
        else if (reportName.startsWith(GCB3INSTALLS))
            result = GCB3INSTALSSTYLESHEET;
        else if (reportName.startsWith(GCB3AUTHCODECHARGES))
            result = GCB3AUTHCODESTYLESHEET;
        else if (reportName.startsWith(GCB3SUNDRY))
            result = GCB3SUNDRYSTYLESHEET;
        else if (reportName.startsWith(GCB3BILLINGNUMBERCHARGES))
            result = GCB3BILLEDNOSUMMARYSTYLESHEET;
        else if (reportName.startsWith(GCB3CALLCHARGES))
            result = GCB3CALLCHARGESSTYLESHEET;
        else if (reportName.startsWith(GCB3CALLINK))
            result = GCB3CALLINKCHARGESSTYLESHEET;
        else if (reportName.startsWith(GCB3EASYQUARTERLY))
            result = GCB3EASYACCESSQUARTERLYCHARGESSTYLESHEET;
        else if (reportName.startsWith(GCB3EASYUSAGECHARGES))
            result = GCB3EASYUSAGECHARGESSTYLESHEET;       
        else if (reportName.startsWith(GCB3SOURCEDISCOUNTS))
            result = GCB3SOURCEDISCOUNTSTYLESHEET;
        else if (reportName.startsWith(GCB3SPECIALCHARGES))
            result = GCB3SPECIALCHARGESSTYLESHEET; 
        else if (reportName.startsWith(GCB3VPNCHARGES))
            result = GCB3VPNCHARGESSTYLESHEET;
        return result;
    }
    
    private String determineParameters(
        String reportName, 
        boolean IsReport,
        String billingSource,   
        String globalCustomerId,
        String invoiceNo,
        String billedProductId,
        String ldFlag )
    {
        String result = "";
        if ((billingSource.startsWith(CONGLOM))&&(IsReport))
        // invoice no plus optional billed product id and LD flag if GCB3 report
        {
            result = SINGLEQUOTE + invoiceNo + SINGLEQUOTE;
            // add billed product id if not invoice report
            if (!reportName.startsWith(GCB3INVOICE))
                result = result + COMMA + SINGLESPACE + SINGLEQUOTE + billedProductId + SINGLEQUOTE;
            // add LD flag is consolidated, billing number summary or usage reports
            if ((reportName.startsWith(GCB3CONSOLIDATED))||
                (reportName.startsWith(GCB3USAGE))||
                (reportName.startsWith(GCB3BILLINGNUMBERCHARGES)))
                result = result + COMMA + SINGLESPACE + SINGLEQUOTE + ldFlag + SINGLEQUOTE;
        }
        else
        // always global customer id and invoice no if not GCB3
            result = 
                SINGLEQUOTE + globalCustomerId + SINGLEQUOTE + 
                COMMA + SINGLESPACE + SINGLEQUOTE + invoiceNo + SINGLEQUOTE;
        return result;
    }
    
    private String fileInvoiceNo( String invoiceNo, String billingSource)
    {        
        String result = invoiceNo;
        int invoiceNoLength = invoiceNo.length();
        if (billingSource.startsWith(CONGLOM))
            result = 
                invoiceNo.substring(0, invoiceNoLength - 5) + 
                "-" + 
                invoiceNo.substring(invoiceNoLength - 4 , invoiceNoLength);
        return result;
    }
        
    // open batch script file
    private boolean openScriptFile(String globalCustomerId, String invoiceNo, String reportName, String billedProductId)
    {
        boolean result = false;
        scriptBW = null;
        String scriptFilename = 
            batchDir + File.separator + 
            globalCustomerId + USCORE + 
            invoiceNo + USCORE +
            reportName + USCORE;
        if (!(billedProductId==null))
            scriptFilename = scriptFilename + billedProductId + USCORE;
        scriptFilename = scriptFilename + fileDT() + BATEXT;
        try
        {
           scriptBW = new BufferedWriter(new FileWriter(scriptFilename)); 
           result = true;
        }
        catch(Exception ex)
        {
           message = "   I/O ERROR: Opening batch script file : " + scriptFilename + " : " + ex.getMessage();
           System.out.println(message);
           writeToLogFile(message);
        }
        return result;
    }
    
    // write line to batch script file
    private boolean writeToScriptFile( String line )
    {
        boolean result = false;
        try
        {
            scriptBW.write(line+"\r\n");
            result = true;
        }
        catch (IOException ex)
        {
           message = "   I/O ERROR: Writing to batch script file : " + ex.getMessage();
           System.out.println(message);
           writeToLogFile(message);
        }
        return result;
    }

    // close script file
    private boolean closeScriptFile()
    {
        boolean result = false;
        try
        {
            scriptBW.close();
            result = true;
        }
        catch (IOException ex)
        {
           message = "   I/O ERROR: Closing batch script file : " + ex.getMessage();
           System.out.println(message);
           writeToLogFile(message);
        }
        return result;
    }
    
    // open log file
    private void openLogFile()
    {
        logBW = null;
        String logDate = new java.util.Date().toString();
        String logFilename = 
            logDir+File.separator+"processInitialReportRequests_"+
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

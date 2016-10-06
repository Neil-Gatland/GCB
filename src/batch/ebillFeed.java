package batch;

import java.io.*;
import java.sql.*;
import java.util.*;

public class ebillFeed
{
  // class variables
  static String billingSource;
  private String dbServer, username, password;
  private String logDir, workDir, extractDir, workDir2;
  private String dbName, bsName;
  private String message;
  private BufferedWriter logBW, workBW, extractBW, work2BW;
  private Connection conn;
  private ebillDB eDB;
  private boolean dbOK, duplicateRun;
  private int candidateCount, processedCount, failedCount;
  private long workFileNo, recordSeqNo, invoiceRecordSeqNo, recordSeqNoReports;
  private String lastMaintainedDate, BPSD, BPED, customerVATRegNo, customerVATRegLiteral, customerVATRegText, panNumber;
  private String qtrlyBilledInd,  qtrlyBilledCycle, vatRate, billDate, invoiceCurrency;
  private String dateProcessed, outgoingCurrencyCode;
  // Feed type constant values
  private final String DATA = "Data";
  private final String CONGLOM = "Conglom";
  private final String GDIAL = "Globaldial";
  // Database constant values
  private final String GCD = "GCD";
  private final String CONGLOMERATE = "Conglomerate";
  private final String DIALUPPRODUCTS = "Dialup_Products";
  // Billing source constant values
  private final String GCBDATA = "GCB Data";
  private final String GLOBALDIAL = "Globaldial";
  // Various text constant values
  private final String USCORE = "_";
  private final String HYPHEN = "-";
  private final String VPIPE = "|";
  private final String EMPTY = "";
  private final String SINGLED = "D";
  private final String SINGLEE = "E";
  private final String SINGLEI = "I";
  private final String SINGLEL = "L";
  private final String SINGLEN = "N";
  private final String SINGLES = "S";
  private final String ZERO = "0";
  private final String ONE = "1";
  private final String NULL = "NULL";
  private final String PERIOD = ".";
  private final String SPACE = " ";
  private final String DATEMS = ".000";
  // Various filename element constant values
  private final String EBILLDATA = "ebilldata";
  private final String EBILLCONGLOM = "ebillconglom";
  private final String EBILLCONGLOMREPORTS = "ebillconglomreports";
  private final String EBILLGDIAL = "ebillgdial";
  private final String FILESUFFIX = ".dat";
  // Record Type constant values
  private final String HEADER = "Header";
  private final String TRAILER = "Trailer";
  private final String ACCOUNT = "Account";
  private final String ADDRESS = "Address";
  private final String BANK = "Bank";
  private final String INVOICE = "Invoice";
  private final String SOURCE = "Source";
  // Address Type constant values
  private final String BILLING = "Billing";
  private final String ALTERNATEBILLING = "Alternate Billing";
  // Reason Code constant values
  private final String MAIN = "Main";
  private final String ALTERNATE = "Alternate";
  private final String REGISTERED = "Registered";
  // Line type constant values
  private final String CHARGE = "Charge";
  private final String ADJUSTMENT = "Adjustment";
  private final String TAX = "Tax";
  // Source data format constant values
  private final String STRING = "String";
  private final String NUMBER = "Number";
  private final String DATE = "Date";
  private final String VAT = "VAT";
  // Local data checking constant values
  private final String CONSOL = "Consol";
  private final String BILSUM = "Bilsum";
  private final String SSVO = "SSVO";
  private final String PCBL = "PCBL";

  private ebillFeed()
  {
    dbServer = "";
    username = "";
    password = "";
    logDir = "";
    workDir = "";
    extractDir = "";
    dbName = "";
  }

  public static void main(String[] args)
  {
    // control processing
    ebillFeed ef = new ebillFeed();
    // get billing source from arguments
    if (args.length!=0)
      billingSource = args[0];
    else
      billingSource = "not supplied";
    ef.control();
  }

  // controls processing
  private void control()
  {
    // get property values
    try
    {
      FileReader properties = new FileReader("ebill.properties");
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
        if (propname.equals("workdir "))
          workDir = propval;
        if (propname.equals("extrdir "))
          extractDir = propval;
        if (propname.equals("workdir2"))
          workDir2 = propval;
        propline = buffer.readLine();
        if (propline == null)
          eofproperties = true;
        else {
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
    // check validity of supplied billing source
    boolean validBS = false;
    if (billingSource.startsWith("not supplied"))
      writeToLogFile("Cannot run as no billing source supplied");
    else if ((!billingSource.startsWith(DATA))&&
             (!billingSource.startsWith(CONGLOM))&&
             (!billingSource.startsWith(GDIAL)))
      writeToLogFile("Cannot run as supplied billing source "+billingSource+" is not valid");
    else
    {
      writeToLogFile("Commencing run for "+billingSource+" at "+currentDT());
      validBS = true;
      dbName = getDBName(billingSource);
      bsName = getBSName(billingSource);
    }
    writeToLogFile(" ");
    // open db connection and ebillDB object
    if (validBS)
    {
      String url = "jdbc:AvenirDriver://"+dbServer+":1433/"+dbName;
      try
      {
        Class.forName("net.avenir.jdbcdriver7.Driver");
        conn = DriverManager.getConnection(url,username,password);
        eDB = new ebillDB(conn);
        dbOK = true;
        message = "   Successfully connected to database "+dbName;
        System.out.println(message);
      }
      catch( Exception e)
      {
        message = "   Error opening database connection : " + e.toString();
        System.out.println(message);
        writeToLogFile(message);
        dbOK = false;
      }
    }
    // check run control and set if not a duplicate run
    if (dbOK)
    {
      if (eDB.running(bsName))
      {
        message = "   Abandoning run as job is already running for this billing source";
        System.out.println(message);
        writeToLogFile(message);
        duplicateRun = true;
      }
      else
        duplicateRun = false;
        eDB.setJobId(billingSource);
    }
    if ((dbOK)&&(!duplicateRun))
    {
      // Billing source specific processing
      candidateCount = 0;
      processedCount = 0;
      failedCount = 0;
      // determine processing on billing system
      if (billingSource.startsWith(DATA))
        dataControl();
      if (billingSource.startsWith(CONGLOM))
        conglomControl();
      if (billingSource.startsWith(GDIAL))
        globaldialControl();
      // check what has been processed and close down database connection
      if (candidateCount==0)
        writeToLogFile("   No invoices identified for feed processing");
      else
      {
        writeToLogFile("   Invoices identified for feed   : "+candidateCount);
        writeToLogFile("   Invoices sucessfully processed : "+processedCount);
        writeToLogFile("   Invoices failed to process     : "+failedCount);
      }
    }
    writeToLogFile("   ");
    if (validBS)
      writeToLogFile("Completing run for "+billingSource+" at "+currentDT());
    if (dbOK)
    {
      if (!duplicateRun)
        eDB.resetJobId(billingSource);
      try
      {
        conn.close();
        message = "   Successfully closed database connection";
        System.out.println(message);
      }
      catch( Exception e)
      {
        message = "   Error closing database connection : " + e.toString();
        System.out.println(message);
        writeToLogFile(message);
      }
    }
    closeLogFile();
  }

  private void globaldialControl()
  {
    boolean ok = true;
    long finalFileNo = 0;
    // get recordset of candidate invoices to count them
    ResultSet rsInvoices = eDB.getGdialInvoiceNos();
    try
    {
      while(rsInvoices.next())
        candidateCount++;
      rsInvoices.close();
    }
    catch(java.sql.SQLException ex)
    {
       message="   failure counting invoices returned by ebill_gdial_invoice_nos : "+ex.getMessage();
       System.out.println(message);
       writeToLogFile(message);
       ok = false;
    }
    if ((candidateCount>0)&&(ok))
    {
      // remove any files in the work directory
      clearWorkDirectory();
      // get last file number used
      long lastFileNo = eDB.getLastFileNo(bsName);
      workFileNo = lastFileNo+1;
      finalFileNo = workFileNo;
      // process candidate invoices
      rsInvoices = eDB.getGdialInvoiceNos();
      try
      {
        while(rsInvoices.next())
        {
          boolean invoiceOK = true;
          String customerReference = rsInvoices.getString("Global_Customer_Id");
          String accountName = rsInvoices.getString("Invoice_Region");
          String invoiceNo = rsInvoices.getString("Invoice_No");
          String workFilename=
            customerReference+USCORE+
            invoiceNo.replace('/','-')+
            FILESUFFIX;
          String extractLine = "";
          recordSeqNo=1;
          openWorkFile(workDir+File.separator+workFilename);
          // invoice header
          extractLine=headerLine(bsName,workFileNo,recordSeqNo,creationDT());
          writeToWorkFile(extractLine);
          recordSeqNo++;
          // invoice account details
            invoiceOK = gdialAccountDetails(customerReference,accountName,invoiceNo);
          // invoice total, charge line and tax line
          if (invoiceOK)
            invoiceOK = gdialInvoice(customerReference,accountName,invoiceNo);
          // usage detail report line and detail
          if (invoiceOK)
            invoiceOK = gDialInvoiceDetail(customerReference,accountName,invoiceNo);
          // invoice trailer
          if (invoiceOK)
          {
            extractLine =
              trailerLine(
                bsName,workFileNo,recordSeqNo,recordSeqNo-2);
            writeToWorkFile(extractLine);
          }
          closeWorkFile();
          // update invoice data to prevent being pulled again
          if (invoiceOK)
            invoiceOK = eDB.setGdialInvoiceExtracted(invoiceNo,workFileNo);
          // check if invoice successfully processed
          if (invoiceOK)
          {
          	message = "      "+customerReference+":"+accountName+":"+invoiceNo+" extracted successfully";
          	System.out.println(message);
          	writeToLogFile(message);
        	writeToLogFile(" ");
            processedCount++;
            finalFileNo=workFileNo;
            workFileNo++;
          }
          else
          {
          	message = "      "+customerReference+":"+accountName+":"+invoiceNo+" not extracted";
          	System.out.println(message);
          	writeToLogFile(message);
        	writeToLogFile(" ");
            failedCount++;
            File wFile = new File(workDir+File.separator+workFilename);
            wFile.delete();
          }
        }
      }
      catch(java.sql.SQLException ex)
      {
        message="   failure processing invoices returned by ebill_dial_invoice_nos : "+ex.getMessage();
        System.out.println(message);
        writeToLogFile(message);
        ok = false;
      }
      // if some invoices have been created but a serious error has occurred
      // then backout update of all the processed invoices
      if ((processedCount>0)&&!(ok))
      {
        boolean backedOutOK = eDB.backoutGdialInvoicesExtracted(lastFileNo);
        if (!(backedOutOK))
        {
          message="   failed to backout invoices processed with file no greater than "+lastFileNo;
          System.out.println(message);
          writeToLogFile(message);
        }
        else
        {
          message="   backed out invoices processed with file no greater than "+lastFileNo;
          System.out.println(message);
          writeToLogFile(message);
        }
      }
    }
    // write out work files to final processed file
    if ((ok)&&(processedCount>0))
    {
      String finalFilename =
        extractDir+File.separator+fileDT()+USCORE+EBILLGDIAL+FILESUFFIX;
      createExtractFile(finalFilename);
      eDB.resetFileNo(finalFileNo,bsName);
    }
  }

  private boolean gDialInvoiceDetail(String globalCustomerId, String accountName, String invoiceNo)
  {
    boolean result = false;
    String workLine = "";
    // supplementary line for globaldial detail
    workLine =
      supplementaryLine(
        bsName,
        workFileNo,
        recordSeqNo,
        globalCustomerId,
        accountName,
        invoiceNo,
        CHARGE,
        dateProcessed,
        "0.00",
        "0.00",
        outgoingCurrencyCode,
        outgoingCurrencyCode,
        "Detail",
        "1.00",
        "",
        GLOBALDIAL);
    writeToWorkFile(workLine);
    recordSeqNo++;
    ResultSet gdialInvoiceDetailRS = eDB.getGdialUsageDets(globalCustomerId,invoiceNo);
    try
    {
      while (gdialInvoiceDetailRS.next())
      {
        String type = tidyString(gdialInvoiceDetailRS.getString("Type"));
        String zone = tidyString(gdialInvoiceDetailRS.getString("Zone"));
        String billingPeriodStartDate = reformatConglomDate(gdialInvoiceDetailRS.getString("Billing_Period_Start_Date"));
        String billingPeriodEndDate = reformatConglomDate(gdialInvoiceDetailRS.getString("Billing_Period_End_Date"));
        String netAmountOutgoing = gdialInvoiceDetailRS.getString("Net_Amount_Outgoing");
        String tariff = tidyString2(gdialInvoiceDetailRS.getString("Tariff"));
        String duration = gdialInvoiceDetailRS.getString("Duration");
        String calls = gdialInvoiceDetailRS.getString("Calls");
        String fixedChargeType = tidyString(gdialInvoiceDetailRS.getString("Fixed_Charge_Type"));
        String fixedChargeDesc = tidyString(gdialInvoiceDetailRS.getString("Fixed_Charge_Desc"));
        String units = tidyString(gdialInvoiceDetailRS.getString("Units"));
        String username = tidyString(gdialInvoiceDetailRS.getString("Username"));
        String chargePeriodStartDate = reformatConglomDate(gdialInvoiceDetailRS.getString("Charge_Period_Start_Date"));
        String chargePeriodEndDate = reformatConglomDate(gdialInvoiceDetailRS.getString("Charge_Period_End_Date"));
        // supplementary line detail for globaldial detail
        workLine =
          supplementaryDetail(
            bsName,
            workFileNo,
            recordSeqNo,
            globalCustomerId,
            accountName,
            invoiceNo,
            CHARGE,
            dateProcessed,
            "0.00",
            "0.00",
            outgoingCurrencyCode,
            outgoingCurrencyCode,
            "Detail",
            "1.00",
            "",
            "Billing Summary");
        writeToWorkFile(workLine);
        recordSeqNo++;
        // source data for globaldial detail
        workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,accountName,invoiceNo,
                      "Type",STRING,"5",EMPTY,type,EMPTY,EMPTY);
        writeToWorkFile(workLine);
        recordSeqNo++;
        workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,accountName,invoiceNo,
                      "Zone",STRING,"30",EMPTY,zone,EMPTY,EMPTY);
        writeToWorkFile(workLine);
        recordSeqNo++;
        workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,accountName,invoiceNo,
                      "Billing_Period_Start_Date",DATE,EMPTY,EMPTY,EMPTY,EMPTY,billingPeriodStartDate);
        writeToWorkFile(workLine);
        recordSeqNo++;
        workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,accountName,invoiceNo,
                      "Billing_Period_End_Date",DATE,EMPTY,EMPTY,EMPTY,EMPTY,billingPeriodEndDate);
        writeToWorkFile(workLine);
        recordSeqNo++;
        workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,accountName,invoiceNo,
                      "Net_Amount_Outgoing",NUMBER,"15","5",EMPTY,netAmountOutgoing,EMPTY);
        writeToWorkFile(workLine);
        recordSeqNo++;
        workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,accountName,invoiceNo,
                      "Tariff",NUMBER,"8","5",EMPTY,tariff,EMPTY);
        writeToWorkFile(workLine);
        recordSeqNo++;
        workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,accountName,invoiceNo,
                      "Duration",NUMBER,"15","5",EMPTY,duration,EMPTY);
        writeToWorkFile(workLine);
        recordSeqNo++;
        workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,accountName,invoiceNo,
                      "Calls",NUMBER,"38","0",EMPTY,calls,EMPTY);
        writeToWorkFile(workLine);
        recordSeqNo++;
        workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,accountName,invoiceNo,
                      "Fixed_Charge_Type",STRING,"30",EMPTY,fixedChargeType,EMPTY,EMPTY);
        writeToWorkFile(workLine);
        recordSeqNo++;
        workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,accountName,invoiceNo,
                      "Fixed_Charge_Desc",STRING,"30",EMPTY,fixedChargeDesc,EMPTY,EMPTY);
        writeToWorkFile(workLine);
        recordSeqNo++;
        workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,accountName,invoiceNo,
                      "Units",NUMBER,"38","0",EMPTY,units,EMPTY);
        writeToWorkFile(workLine);
        recordSeqNo++;
        workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,accountName,invoiceNo,
                      "Username",STRING,"50",EMPTY,username,EMPTY,EMPTY);
        writeToWorkFile(workLine);
        recordSeqNo++;
        workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,accountName,invoiceNo,
                      "Charge_Period_Start_Date",DATE,EMPTY,EMPTY,EMPTY,EMPTY,chargePeriodStartDate);
        writeToWorkFile(workLine);
        recordSeqNo++;
        workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,accountName,invoiceNo,
                      "Charge_Period_End_Date",DATE,EMPTY,EMPTY,EMPTY,EMPTY,chargePeriodEndDate);
        writeToWorkFile(workLine);
        recordSeqNo++;
      }
      result = true;
    }
    catch(java.sql.SQLException ex)
    {
      message =
        "   Error getting globaldial_invoice_details for "+globalCustomerId+" : "+invoiceNo+ex.toString();
      System.out.println(message);
      writeToLogFile(message);
    }
    finally
    {
      gdialInvoiceDetailRS = null;
    }

    return result;
  }

  private boolean gdialInvoice(String globalCustomerId, String accountName, String invoiceNo)
  {
    boolean result = false;
    String workLine = "";
    String invoiceTotal = "0.00", billingPeriodStartDate = "", billingPeriodEndDate = "";
    String netAmount = "0.00", outgoingCurrencyCode = "", jurisdictionExRate = "1.00";
    String globalAccountManager = "", customerName = "", reportingRegion = "", billingType = "";
    String dateOfProcessing = "", earliestUsageDate = "", latestUsageDate = "", translationDate = "";
    String chargePeriodStartDate = "", chargePeriodEndDate = "";
    String netAmountReference = "0.00", topN = "0", exrateOutgoingToGBP = "0.00";
    String tax = "0.00", taxRate = "0.00";
    String taxType = "", taxDescription = "", sapTaxCode = "";
    // get invocie data from globdial invoice and data stored procedures
    ResultSet globaldialInvoiceRS = eDB.getGdialInvoiceDets(globalCustomerId,invoiceNo);
    try
    {
      if (globaldialInvoiceRS.next())
      {
        billingPeriodStartDate = reformatConglomDate(globaldialInvoiceRS.getString("Billing_Period_Start_Date"));
        billingPeriodEndDate = reformatConglomDate(globaldialInvoiceRS.getString("Billing_Period_End_Date"));
        dateProcessed = reformatConglomDate(globaldialInvoiceRS.getString("Date_Processed"));
        netAmount = globaldialInvoiceRS.getString("Net_Amount");
        outgoingCurrencyCode = globaldialInvoiceRS.getString("Outgoing_Currency_Code");
        globalAccountManager = tidyString(globaldialInvoiceRS.getString("Global_Account_Manager"));
        customerName = tidyString(globaldialInvoiceRS.getString("Customer_Name"));
        dateOfProcessing = reformatConglomDate(globaldialInvoiceRS.getString("Date_of_Processing"));
        reportingRegion = tidyString(globaldialInvoiceRS.getString("Reporting_Region"));
        billingType = tidyString(globaldialInvoiceRS.getString("Billing_Type"));
        earliestUsageDate = reformatConglomDate(globaldialInvoiceRS.getString("Earliest_Usage_Date"));
        latestUsageDate = reformatConglomDate(globaldialInvoiceRS.getString("Latest_Usage_Date"));
        topN = globaldialInvoiceRS.getString("Top_N");
        chargePeriodStartDate = reformatConglomDate(globaldialInvoiceRS.getString("Charge_Period_Start_Date"));
        chargePeriodEndDate = reformatConglomDate(globaldialInvoiceRS.getString("Charge_Period_End_Date"));
        tax = globaldialInvoiceRS.getString("Tax");
        taxRate = tidyString2(globaldialInvoiceRS.getString("Tax_Rate"));
        taxType = tidyString(globaldialInvoiceRS.getString("Tax_Type"));
        taxDescription = tidyString(globaldialInvoiceRS.getString("Tax_Description"));
        jurisdictionExRate = globaldialInvoiceRS.getString("Jurisdiction_ExRate");
      }
    }
    catch(java.sql.SQLException ex)
    {
      message =
        "   Error getting globaldial_invoice details for "+globalCustomerId+" : "+accountName+ex.toString();
      System.out.println(message);
      writeToLogFile(message);
    }
    finally
    {
      globaldialInvoiceRS = null;
    }
    ResultSet gdialInvoiceRS = eDB.getGdialData(globalCustomerId,invoiceNo);
    try
    {
      if (gdialInvoiceRS.next())
      {
        invoiceTotal = gdialInvoiceRS.getString("Invoice_Total");
        netAmountReference = gdialInvoiceRS.getString("Net_Amount_Reference");
        exrateOutgoingToGBP = tidyString2(gdialInvoiceRS.getString("ExRate_Outgoing_To_GBP"));
        translationDate = reformatConglomDate(gdialInvoiceRS.getString("Translation_Date"));
        sapTaxCode = tidyString(gdialInvoiceRS.getString("SAP_Tax_Code"));
      }
    }
    catch(java.sql.SQLException ex)
    {
      message =
        "   Error getting ebill_gdial_data details for "+globalCustomerId+" : "+accountName+ex.toString();
      System.out.println(message);
      writeToLogFile(message);
    }
    finally
    {
      gdialInvoiceRS = null;
    }
    // invoice total line
    workLine =
      invoiceTotalLine
        (bsName,
         workFileNo,
         recordSeqNo,
         globalCustomerId,
         accountName,
         invoiceNo,
         dateProcessed,
         dateProcessed,
         billingPeriodStartDate,
         billingPeriodEndDate,
         invoiceTotal,
         GLOBALDIAL);
    writeToWorkFile(workLine);
    recordSeqNo++;
    // invoice charge line
    workLine =
      invoiceLine
        (bsName,
         workFileNo,
         recordSeqNo,
         globalCustomerId,
         accountName,
         invoiceNo,
         CHARGE,
         dateProcessed,
         netAmount,
         netAmount,
         outgoingCurrencyCode,
         outgoingCurrencyCode,
         "Monthly Charges",
         "1.00",
         "",
         GLOBALDIAL);
    writeToWorkFile(workLine);
    recordSeqNo++;
    // source data for invoice charge line
    workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,accountName,invoiceNo,
                  "Global_Account_Manager",STRING,"25",EMPTY,globalAccountManager,EMPTY,EMPTY);
    writeToWorkFile(workLine);
    recordSeqNo++;
    workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,accountName,invoiceNo,
                  "Customer_Name",STRING,"25",EMPTY,customerName,EMPTY,EMPTY);
    writeToWorkFile(workLine);
    recordSeqNo++;
    workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,accountName,invoiceNo,
                  "Billing_Period_Start_Date",DATE,EMPTY,EMPTY,EMPTY,EMPTY,billingPeriodStartDate);
    writeToWorkFile(workLine);
    recordSeqNo++;
    workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,accountName,invoiceNo,
                  "Billing_Period_End_Date",DATE,EMPTY,EMPTY,EMPTY,EMPTY,billingPeriodEndDate);
    writeToWorkFile(workLine);
    recordSeqNo++;
    workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,accountName,invoiceNo,
                  "Date_of_Processing",DATE,EMPTY,EMPTY,EMPTY,EMPTY,dateOfProcessing);
    writeToWorkFile(workLine);
    recordSeqNo++;
    workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,accountName,invoiceNo,
                  "Date_Processed",DATE,EMPTY,EMPTY,EMPTY,EMPTY,dateProcessed);
    writeToWorkFile(workLine);
    recordSeqNo++;
    workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,accountName,invoiceNo,
                  "Reporting_Region",STRING,"50",EMPTY,reportingRegion,EMPTY,EMPTY);
    writeToWorkFile(workLine);
    recordSeqNo++;
    workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,accountName,invoiceNo,
                  "Billing_Type",STRING,"6",EMPTY,billingType,EMPTY,EMPTY);
    writeToWorkFile(workLine);
    recordSeqNo++;
    workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,accountName,invoiceNo,
                  "Earliest_Usage_Date",DATE,EMPTY,EMPTY,EMPTY,EMPTY,earliestUsageDate);
    writeToWorkFile(workLine);
    recordSeqNo++;
    workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,accountName,invoiceNo,
                  "Latest_Usage_Date",DATE,EMPTY,EMPTY,EMPTY,EMPTY,latestUsageDate);
    writeToWorkFile(workLine);
    recordSeqNo++;
    workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,accountName,invoiceNo,
                  "Net_Amount_Reference",NUMBER,"15","5",EMPTY,netAmountReference,EMPTY);
    writeToWorkFile(workLine);
    recordSeqNo++;
    workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,accountName,invoiceNo,
                  "Top_N",NUMBER,"39",EMPTY,EMPTY,topN,EMPTY);
    writeToWorkFile(workLine);
    recordSeqNo++;
    workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,accountName,invoiceNo,
                  "ExRate_Outgoing_To_GBP",NUMBER,"22","7",EMPTY,exrateOutgoingToGBP,EMPTY);
    writeToWorkFile(workLine);
    recordSeqNo++;
    workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,accountName,invoiceNo,
                  "Translation_Date",DATE,EMPTY,EMPTY,EMPTY,EMPTY,translationDate);
    writeToWorkFile(workLine);
    recordSeqNo++;
    workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,accountName,invoiceNo,
                  "Charge_Period_Start_Date",DATE,EMPTY,EMPTY,EMPTY,EMPTY,chargePeriodStartDate);
    writeToWorkFile(workLine);
    recordSeqNo++;
    workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,accountName,invoiceNo,
                  "Charge_Period_End_Date",DATE,EMPTY,EMPTY,EMPTY,EMPTY,chargePeriodEndDate);
    writeToWorkFile(workLine);
    recordSeqNo++;
    // invoice taxe line
    workLine =
      invoiceLine
        (bsName,
         workFileNo,
         recordSeqNo,
         globalCustomerId,
         accountName,
         invoiceNo,
         TAX,
         dateProcessed,
         tax,
         tax,
         outgoingCurrencyCode,
         outgoingCurrencyCode,
         "Tax in "+outgoingCurrencyCode,
         "1.00",
         taxRate,
         GLOBALDIAL);
    writeToWorkFile(workLine);
    recordSeqNo++;
    // source data for invoice tax line
    workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,accountName,invoiceNo,
                  "Tax_Type",STRING,"6",EMPTY,taxType,EMPTY,EMPTY);
    writeToWorkFile(workLine);
    recordSeqNo++;
    workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,accountName,invoiceNo,
                  "Tax_Description",STRING,"100",EMPTY,taxDescription,EMPTY,EMPTY);
    writeToWorkFile(workLine);
    recordSeqNo++;
    workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,accountName,invoiceNo,
                  "Tax_Rate",NUMBER,"15","5",EMPTY,taxRate,EMPTY);
    writeToWorkFile(workLine);
    recordSeqNo++;
    workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,accountName,invoiceNo,
                  "SAP_Tax_Code",STRING,"5",EMPTY,sapTaxCode,EMPTY,EMPTY);
    writeToWorkFile(workLine);
    recordSeqNo++;
    workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,accountName,invoiceNo,
                  "Jurisdiction_ExRate",NUMBER,"15","7",EMPTY,jurisdictionExRate,EMPTY);
    writeToWorkFile(workLine);
    recordSeqNo++;
    result = true;
    return result;
  }

  private boolean gdialAccountDetails(String globalCustomerId, String accountName, String invoiceNo)
  {
    boolean result = false;
    String workLine = "";
    ResultSet accountDetailsRS = eDB.getGdialAccountDets(globalCustomerId,accountName);
    try
    {
      if (accountDetailsRS.next())
      {
        String lastMaintainedDate = accountDetailsRS.getString("Last_Maintained_Date");
        String generalLedger = accountDetailsRS.getString("General_Ledger");
        String sapCustomerNumber = accountDetailsRS.getString("SAP_Customer_Number");
        String glCode = accountDetailsRS.getString("GL_Code");
        outgoingCurrencyCode = accountDetailsRS.getString("Outgoing_Currency_Code");
        String companyName = accountDetailsRS.getString("Company_Name");
        String billingContact = accountDetailsRS.getString("Billing_Contact");
        String billingCustomerName = accountDetailsRS.getString("Billing_Customer_Name");
        String billingAddress1 = accountDetailsRS.getString("Billing_Address1");
        String billingAddress2 = accountDetailsRS.getString("Billing_Address2");
        String billingAddress3 = accountDetailsRS.getString("Billing_Address3");
        String billingAddress4 = accountDetailsRS.getString("Billing_Address4");
        String billingAddress5 = accountDetailsRS.getString("Billing_Address5");
        String billingAddress6 = accountDetailsRS.getString("Billing_Address6");
        String billingAddress7 = accountDetailsRS.getString("Billing_Address7");
        String altBillingAddress1 = accountDetailsRS.getString("Alt_Billing_Address1");
        String altBillingAddress2 = accountDetailsRS.getString("Alt_Billing_Address2");
        String altBillingAddress3 = accountDetailsRS.getString("Alt_Billing_Address3");
        String altBillingAddress4 = accountDetailsRS.getString("Alt_Billing_Address4");
        String altBillingAddress5 = accountDetailsRS.getString("Alt_Billing_Address5");
        String altBillingAddress6 = accountDetailsRS.getString("Alt_Billing_Address6");
        String altBillingAddress7 = accountDetailsRS.getString("Alt_Billing_Address7");
        String bankAddress1 = accountDetailsRS.getString("Bank_Address1");
        String bankAddress2 = accountDetailsRS.getString("Bank_Address2");
        String bankAddress3 = accountDetailsRS.getString("Bank_Address3");
        String bankAddress4 = accountDetailsRS.getString("Bank_Address4");
        String bankAddress5 = accountDetailsRS.getString("Bank_Address5");
        String bankAddress6 = accountDetailsRS.getString("Bank_Address6");
        String bankAddress7 = accountDetailsRS.getString("Bank_Address7");
        String altBankAddress1 = accountDetailsRS.getString("Alt_Bank_Address1");
        String altBankAddress2 = accountDetailsRS.getString("Alt_Bank_Address2");
        String altBankAddress3 = accountDetailsRS.getString("Alt_Bank_Address3");
        String altBankAddress4 = accountDetailsRS.getString("Alt_Bank_Address4");
        String altBankAddress5 = accountDetailsRS.getString("Alt_Bank_Address5");
        String altBankAddress6 = accountDetailsRS.getString("Alt_Bank_Address6");
        String altBankAddress7 = accountDetailsRS.getString("Alt_Bank_Address7");
        String accountReference = accountDetailsRS.getString("Account_Reference");
        String taxReferenceLiteral = tidyString(accountDetailsRS.getString("Tax_Reference_Literal"));
        String taxReference = tidyString(accountDetailsRS.getString("Tax_Reference"));
        String jurisidictionCurrencyCode = accountDetailsRS.getString("Jurisidiction_Currency_Code");
        String customerContactPoint = accountDetailsRS.getString("Customer_Contact_Point");
        String countryCode = accountDetailsRS.getString("Country_Code");
        customerVATRegNo = accountDetailsRS.getString("Customer_VAT_Reg_No");
        customerVATRegLiteral = accountDetailsRS.getString("Customer_VAT_Reg_Literal");
        customerVATRegText = accountDetailsRS.getString("Customer_VAT_Reg_Text");
        String customerRegisteredAddress1 = accountDetailsRS.getString("Customer_Registered_Address1");
        String customerRegisteredAddress2 = accountDetailsRS.getString("Customer_Registered_Address2");
        String customerRegisteredAddress3 = accountDetailsRS.getString("Customer_Registered_Address3");
        String customerRegisteredAddress4 = accountDetailsRS.getString("Customer_Registered_Address4");
        String customerRegisteredAddress5 = accountDetailsRS.getString("Customer_Registered_Address5");
        // account record
        workLine =
          accountLine(
            bsName,
            workFileNo,
            recordSeqNo,
            globalCustomerId,
            accountName,
            invoiceNo,
            lastMaintainedDate,
            generalLedger,
            glCode,
            sapCustomerNumber,
            outgoingCurrencyCode,
            companyName,
            billingContact,
            billingCustomerName,
            "",
            customerContactPoint,
            accountReference,
            taxReferenceLiteral,
            taxReference,
            jurisidictionCurrencyCode);
        writeToWorkFile(workLine);
        recordSeqNo++;
        // billing address line
        workLine =
          addressLine(
            bsName,
            workFileNo,
            recordSeqNo,
            globalCustomerId,
            accountName,
            invoiceNo,
            BILLING,
            lastMaintainedDate,
            billingAddress1,
            billingAddress2,
            billingAddress3,
            billingAddress4,
            billingAddress5,
            billingAddress6,
            billingAddress7,
            "",
            "",
            countryCode);
        writeToWorkFile(workLine);
        recordSeqNo++;
        // alternate billing address line
        if (altBillingAddress1.length()>0)
        {
          workLine =
            addressLine(
              bsName,
              workFileNo,
              recordSeqNo,
              globalCustomerId,
              accountName,
              invoiceNo,
              ALTERNATEBILLING,
              lastMaintainedDate,
              altBillingAddress1,
              altBillingAddress2,
              altBillingAddress3,
              altBillingAddress4,
              altBillingAddress5,
              altBillingAddress6,
              altBillingAddress7,
              "",
              "",
              "");
          writeToWorkFile(workLine);
          recordSeqNo++;
        }
        // bank address line
        workLine =
          bankLine(
            bsName,
            workFileNo,
            recordSeqNo,
            globalCustomerId,
            accountName,
            invoiceNo,
            MAIN,
            lastMaintainedDate,
            bankAddress1,
            bankAddress2,
            bankAddress3,
            bankAddress4,
            bankAddress5,
            bankAddress6,
            bankAddress7,
            "",
            "",
          "");
        writeToWorkFile(workLine);
        recordSeqNo++;
        // alternate bank address line
        workLine =
          bankLine(
            bsName,
            workFileNo,
            recordSeqNo,
            globalCustomerId,
            accountName,
            invoiceNo,
            ALTERNATE,
            lastMaintainedDate,
            altBankAddress1,
            altBankAddress2,
            altBankAddress3,
            altBankAddress4,
            altBankAddress5,
            altBankAddress6,
            altBankAddress7,
            "",
            "",
            "");
        writeToWorkFile(workLine);
        recordSeqNo++;
        if (customerRegisteredAddress1.length()>0)
        {
          // registered address line
          workLine =
            addressLine(
              bsName,
              workFileNo,
              recordSeqNo,
              globalCustomerId,
              accountName,
              invoiceNo,
              REGISTERED,
              lastMaintainedDate,
              customerRegisteredAddress1,
              customerRegisteredAddress2,
              customerRegisteredAddress3,
              customerRegisteredAddress4,
              customerRegisteredAddress5,
              "",
              "",
              "",
              "",
              countryCode);
          writeToWorkFile(workLine);
          recordSeqNo++;
        }
        result = true;
      }
    }
    catch(java.sql.SQLException ex)
    {
      message =
        "   Error getting account details for account "+globalCustomerId+" : "+accountName+
        " for globaldial invoice "+invoiceNo+" : "+ex.toString();
      System.out.println(message);
      writeToLogFile(message);
    }
    finally
    {
      accountDetailsRS = null;
    }
    return result;
  }

  // control processing for a conglom feed
  private void conglomControl()
  {
    boolean ok = true;
    long finalFileNo = 0;
    // get recordset of candidate invoices to count them
    ResultSet rsInvoices = eDB.getConglomInvoiceNos();
    try
    {
      while(rsInvoices.next())
        candidateCount++;
      rsInvoices.close();
    }
    catch(java.sql.SQLException ex)
    {
       message="   failure counting invoices returned by ebill_conglom_invoice_nos : "+ex.getMessage();
       System.out.println(message);
       writeToLogFile(message);
       ok = false;
    }
    if ((candidateCount>0)&&(ok))
    {
      // remove any files in the work directories
      clearWorkDirectory();
      clearWork2Directory();
      // get last file number used
      long lastFileNo = eDB.getLastFileNo(bsName);
      workFileNo = lastFileNo+1;
      finalFileNo = workFileNo;
      // process candidate invoices
      rsInvoices = eDB.getConglomInvoiceNos();
      try
      {
        while(rsInvoices.next())
        {
          boolean invoiceOK = true;
          String customerReference=rsInvoices.getString("Customer_Reference");
          String accountName=rsInvoices.getString("Account_Name");
          String invoiceNo=rsInvoices.getString("Invoice_No");
          String workFilename=
            customerReference+USCORE+
            invoiceNo.replace('/','-')+
            FILESUFFIX;
          String work2Filename=
            customerReference+USCORE+
            invoiceNo.replace('/','-')+
            FILESUFFIX;
          String extractLine = "";
          recordSeqNo=1;
          recordSeqNoReports=1;
          openWorkFile(workDir+File.separator+workFilename);
          openWork2File(workDir2+File.separator+work2Filename);
          // invoice header
          extractLine=headerLine(bsName,workFileNo,recordSeqNo,creationDT());
          writeToWorkFile(extractLine);
          writeToWork2File(extractLine);
          recordSeqNo++;
          recordSeqNoReports++;
          // invoice account detail lines
          invoiceOK = conglomAccountDetails(customerReference,accountName,invoiceNo);
          // invoice detail lines
          if (invoiceOK)
            invoiceOK = conglomInvoiceDetails(customerReference,accountName,invoiceNo);
          // various reports
          if (invoiceOK)
            invoiceOK = conglomSwitchedConsolNoLD(customerReference,accountName,invoiceNo);
          if (invoiceOK)
            invoiceOK = conglomSwitchedConsolLD(customerReference,accountName,invoiceNo);
          if (invoiceOK)
            invoiceOK = conglomSwitchedBilledNoSummaryNoLD(customerReference,accountName,invoiceNo);
          if (invoiceOK)
            invoiceOK = conglomSwitchedBilledNoSummaryLD(customerReference,accountName,invoiceNo);
          if (invoiceOK)
            invoiceOK = conglomSwitchedCallChargesSummary(customerReference,accountName,invoiceNo);
          if (invoiceOK)
            invoiceOK = conglomSwitchedRentals(customerReference,accountName,invoiceNo);
          if (invoiceOK)
            invoiceOK = conglomSwitchedSourceDiscount(customerReference,accountName,invoiceNo);
          if (invoiceOK)
            invoiceOK = conglomSwitchedCallink(customerReference,accountName,invoiceNo);
          if (invoiceOK)
            invoiceOK = conglomSwitchedAdjustments(customerReference,accountName,invoiceNo);
          if (invoiceOK)
            invoiceOK = conglomSwitchedEasyUsageCharges(customerReference,accountName,invoiceNo);
          if (invoiceOK)
            invoiceOK = conglomSwitchedInstallCharges(customerReference,accountName,invoiceNo);
          if (invoiceOK)
            invoiceOK = conglomSwitchedSundry(customerReference,accountName,invoiceNo);
          if (invoiceOK)
            invoiceOK = conglomSwitchedVPN(customerReference,accountName,invoiceNo);
          if (invoiceOK)
            invoiceOK = conglomSwitchedUsageNOLD(customerReference,accountName,invoiceNo);
          if (invoiceOK)
            invoiceOK = conglomSwitchedUsageLD(customerReference,accountName,invoiceNo);
          if (invoiceOK)
            invoiceOK = conglomLeasedConsolNOLD(customerReference,accountName,invoiceNo);
          if (invoiceOK)
            invoiceOK = conglomLeasedConsolLD(customerReference,accountName,invoiceNo);
          if (invoiceOK)
            invoiceOK = conglomLeasedInstalls(customerReference,accountName,invoiceNo);
          if (invoiceOK)
            invoiceOK = conglomLeasedSundry(customerReference,accountName,invoiceNo);
          if (invoiceOK)
            invoiceOK = conglomLeasedRentals(customerReference,accountName,invoiceNo);
          if (invoiceOK)
            invoiceOK = conglomLeasedAdjustments(customerReference,accountName,invoiceNo);
          if (invoiceOK)
            invoiceOK = conglomSwitchedAuthcodeCharges(customerReference,accountName,invoiceNo);
          if (invoiceOK)
            invoiceOK = conglomSwitchedEasyQuarterly(customerReference,accountName,invoiceNo);
          if (invoiceOK)
            invoiceOK = conglomSwitchedSpecials(customerReference,accountName,invoiceNo);
          if (invoiceOK)
            invoiceOK = conglomSwitchedDiscountSummary(customerReference,accountName,invoiceNo);
          if (invoiceOK)
            invoiceOK = ebillConglomDownload(customerReference,accountName,invoiceNo);
          if (invoiceOK)
            invoiceOK = conglomTalismanOutbound(customerReference,accountName,invoiceNo);
          if (invoiceOK)
            invoiceOK = conglomTalismanInbound(customerReference,accountName,invoiceNo);
          if (invoiceOK)
            invoiceOK = conglomEbillBilledNoSummary(customerReference,accountName,invoiceNo);
          if (invoiceOK)
            invoiceOK = conglomArborTalismanData(customerReference,accountName,invoiceNo);
          // invoice trailer
          if (invoiceOK)
          {
            extractLine =
              trailerLine(
                bsName,workFileNo,recordSeqNo,recordSeqNo-2);
            writeToWorkFile(extractLine);
            extractLine =
              trailerLine(
                bsName,workFileNo,recordSeqNoReports,recordSeqNoReports-2);
            writeToWork2File(extractLine);
          }
          closeWorkFile();
          closeWork2File();
          // update invoice data to prevent being pulled again
          if (invoiceOK)
            invoiceOK = eDB.setConglomInvoiceExtracted(invoiceNo,workFileNo);
          // check if invoice successfully processed
          if (invoiceOK)
          {
          	message = "      "+customerReference+":"+accountName+":"+invoiceNo+" extracted successfully";
          	System.out.println(message);
          	writeToLogFile(message);
          	writeToLogFile(" ");
            processedCount++;
            finalFileNo=workFileNo;
            workFileNo++;
          }
          else
          {
        	message = "      "+customerReference+":"+accountName+":"+invoiceNo+" not extracted";
          	System.out.println(message);
          	writeToLogFile(message);
          	writeToLogFile(" ");
            failedCount++;
            File wFile = new File(workDir+File.separator+workFilename);
            wFile.delete();
            File w2File = new File(workDir2+File.separator+work2Filename);
            w2File.delete();
          }
        }
      }
      catch(java.sql.SQLException ex)
      {
        message="   failure processing invoices returned by ebill_conglom_invoice_nos : "+ex.getMessage();
        System.out.println(message);
        writeToLogFile(message);
        ok = false;
      }
      // if some invoices have been created but a serious error has occurred
      // then backout update of all the processed invoices
      if ((processedCount>0)&&!(ok))
      {
        boolean backedOutOK = eDB.backoutConglomInvoicesExtracted(lastFileNo);
        if (!(backedOutOK))
        {
          message="   failed to backout invoices processed with file no greater than "+lastFileNo;
          System.out.println(message);
          writeToLogFile(message);
        }
        else
        {
          message="   backed out invoices processed with file no greater than "+lastFileNo;
          System.out.println(message);
          writeToLogFile(message);
        }
      }
    }
    // write out work files to final processed file
    if ((ok)&&(processedCount>0))
    {
      String finalFilename =
        extractDir+File.separator+fileDT()+USCORE+EBILLCONGLOM+FILESUFFIX;
      createExtractFile(finalFilename);
      String final2Filename =
        extractDir+File.separator+fileDT()+USCORE+EBILLCONGLOMREPORTS+FILESUFFIX;
      createExtract2File(final2Filename);
      eDB.resetFileNo(finalFileNo,bsName);
    }
  }

  // control processing for a data feed
  private void dataControl()
  {
    boolean ok = true;
    long finalFileNo=0;
    // get recordset of candidate invoices to count invoices
    //ResultSet rsInvoicesCount = eDB.getDataInvoiceNos();
    ResultSet rsInvoices = eDB.getDataInvoiceNos();
    try
    {
      while(rsInvoices.next())
        candidateCount++;
      rsInvoices.close();
    }
    catch(java.sql.SQLException ex)
    {
       message="   failure counting invoices returned by ebill_data_invoice_nos";
       System.out.println(message);
       writeToLogFile(message);
       ok = false;
    }
    if ((candidateCount>0)&&(ok))
    {
      // remove any files in work directory
      clearWorkDirectory();
      // get last file number used
      long lastFileNo = eDB.getLastFileNo(bsName);
      workFileNo = lastFileNo+1;
      finalFileNo = workFileNo;
      // process candidate invoices
      rsInvoices = eDB.getDataInvoiceNos();
      try
      {
        while(rsInvoices.next())
        {
          boolean invoiceOK = true;
          String invoiceNo=rsInvoices.getString("Invoice_No");
          String globalCustomerId=rsInvoices.getString("Global_Customer_Id");
          String invoiceRegion=rsInvoices.getString("Invoice_Region");
          String workFilename=
            globalCustomerId+USCORE+
            invoiceNo+
            FILESUFFIX;
          String extractLine = "";
          recordSeqNo=1;
          openWorkFile(workDir+File.separator+workFilename);
          // invoice header
          extractLine =
            headerLine(
              bsName,workFileNo,recordSeqNo,creationDT());
          writeToWorkFile(extractLine);
          recordSeqNo++;
          //System.out.println("Prior to account dets");
          // invoice account detail lines
          invoiceOK = dataAccountDetails(globalCustomerId,invoiceRegion,invoiceNo);
          //System.out.println("After account dets");
          // save current record seq no for invoice total produced at end
          invoiceRecordSeqNo = recordSeqNo;
          recordSeqNo++;
          // invoice charge lines
          if (invoiceOK)
            invoiceOK = dataChargeDetails(globalCustomerId,invoiceRegion,invoiceNo);
          //System.out.println("After charge lines");
          // invoice tax lines
          if (invoiceOK)
            invoiceOK = dataTaxDetails(globalCustomerId,invoiceRegion,invoiceNo);
          //System.out.println("After tax lines");
          // invoice adjustment lines
          if (invoiceOK)
            invoiceOK = dataAdjustmentDetails(globalCustomerId,invoiceRegion,invoiceNo);
          //System.out.println("After adjustment lines");
          // discount supplementary data
          if (invoiceOK)
            invoiceOK = dataDiscountDetails(globalCustomerId,invoiceRegion,invoiceNo);
          //System.out.println("After discount lines");
          // invoice total line
          if (invoiceOK)
            invoiceOK = dataInvoiceTotal(globalCustomerId,invoiceRegion,invoiceNo);
          //System.out.println("After total lines");
          // invoice trailer
          if (invoiceOK)
          {
            extractLine =
              trailerLine(
                bsName,workFileNo,recordSeqNo,recordSeqNo-2);
            writeToWorkFile(extractLine);
          }
          //System.out.println("After trailer");
          closeWorkFile();
          // update invoice data to prevent being pulled again
          if (invoiceOK)
          {
            //System.out.println("Prior to update as extracted");
            invoiceOK = eDB.setDataInvoiceExtracted(globalCustomerId,invoiceNo,workFileNo);
            //System.out.println("After update to extracted");
          }
          // check if invoice successfully processed
          if (invoiceOK)
          {
        	message = "      "+globalCustomerId+":"+invoiceRegion+":"+invoiceNo+" extracted successfully";
        	System.out.println(message);
        	writeToLogFile(message);
        	writeToLogFile(" ");
            processedCount++;
            finalFileNo=workFileNo;
            workFileNo++;
          }
          else
          {
        	message = "      "+globalCustomerId+":"+invoiceRegion+":"+invoiceNo+" not extracted";
          	System.out.println(message);
          	writeToLogFile(message);
        	writeToLogFile(" ");
            failedCount++;
            File wFile = new File(workDir+File.separator+workFilename);
            wFile.delete();
          }
        }
      }
      catch(java.sql.SQLException ex)
      {
        message="   failure processing invoices returned by ebill_data_invoice_nos : "+ex.getMessage();
        System.out.println(message);
        writeToLogFile(message);
        ok = false;
      }
      rsInvoices=null;
      // if some invoices have been created but a serious error has occurred
      // then backout update of all the processed invoices
      if ((processedCount>0)&&!(ok))
      {
        boolean backedOutOK = eDB.backoutDataInvoicesExtracted(lastFileNo);
        if (!(backedOutOK))
        {
          message="   failed to backout invoice processed with file no greater than "+lastFileNo;
          System.out.println(message);
          writeToLogFile(message);
        }
        else
        {
          message="   backed out invoices processed with file no greater than "+lastFileNo;
          System.out.println(message);
          writeToLogFile(message);
        }
      }
    }
    // write out work files to final processed file
    if ((ok)&&(processedCount>0))
    {
      String finalFilename =
        extractDir+File.separator+fileDT()+USCORE+EBILLDATA+FILESUFFIX;
      createExtractFile(finalFilename);
      eDB.resetFileNo(finalFileNo,bsName);
    }
  }

  private boolean dataAccountDetails(String globalCustomerId, String invoiceRegion, String invoiceNo)
  {
    boolean result = false;
    String workLine = "";
    ResultSet accountDetailsRS = eDB.getDataAccountDets(globalCustomerId,invoiceNo);
    try
    {
      if (accountDetailsRS.next())
      {
        lastMaintainedDate = tidyString(accountDetailsRS.getString("Last_Maintained_Date"));
        String generalLedger = tidyString(accountDetailsRS.getString("General_Ledger"));
        String SAPCustomerNumber = tidyString(accountDetailsRS.getString("SAP_Customer_Number"));
        String GLCode = tidyString(accountDetailsRS.getString("GL_Code"));
        String outgoingCurrencyCode = tidyString(accountDetailsRS.getString("Outgoing_Currency_Code"));
        String companyName = tidyString(accountDetailsRS.getString("Company_Name"));
        String billingContact = tidyString(accountDetailsRS.getString("Billing_Contact"));
        String billingCustomerName = tidyString(accountDetailsRS.getString("Billing_Customer_Name"));
        String billingAddress1 = tidyString(accountDetailsRS.getString("Billing_Address1"));
        String billingAddress2 = tidyString(accountDetailsRS.getString("Billing_Address2"));
        String billingAddress3 = tidyString(accountDetailsRS.getString("Billing_Address3"));
        String billingAddress4 = tidyString(accountDetailsRS.getString("Billing_Address4"));
        String billingAddress5 = tidyString(accountDetailsRS.getString("Billing_Address5"));
        String billingAddress6 = tidyString(accountDetailsRS.getString("Billing_Address6"));
        String billingAddress7 = tidyString(accountDetailsRS.getString("Billing_Address7"));
        String bankAddress1 = tidyString(accountDetailsRS.getString("Bank_Address1"));
        String bankAddress2 = tidyString(accountDetailsRS.getString("Bank_Address2"));
        String bankAddress3 = tidyString(accountDetailsRS.getString("Bank_Address3"));
        String bankAddress4 = tidyString(accountDetailsRS.getString("Bank_Address4"));
        String bankAddress5 = tidyString(accountDetailsRS.getString("Bank_Address5"));
        String bankAddress6 = tidyString(accountDetailsRS.getString("Bank_Address6"));
        String bankAddress7 = tidyString(accountDetailsRS.getString("Bank_Address7"));
        String altBankAddress1 = tidyString(accountDetailsRS.getString("Alt_Bank_Address1"));
        String altBankAddress2 = tidyString(accountDetailsRS.getString("Alt_Bank_Address2"));
        String altBankAddress3 = tidyString(accountDetailsRS.getString("Alt_Bank_Address3"));
        String altBankAddress4 = tidyString(accountDetailsRS.getString("Alt_Bank_Address4"));
        String altBankAddress5 = tidyString(accountDetailsRS.getString("Alt_Bank_Address5"));
        String altBankAddress6 = tidyString(accountDetailsRS.getString("Alt_Bank_Address6"));
        String altBankAddress7 = tidyString(accountDetailsRS.getString("Alt_Bank_Address7"));
        String accountReference = tidyString(accountDetailsRS.getString("Account_Reference"));
        String taxReferenceLiteral = tidyString(accountDetailsRS.getString("Tax_Reference_Literal"));
        String taxReference = tidyString(accountDetailsRS.getString("Tax_Reference"));
        String jurisdictionCurrencyCode = tidyString(accountDetailsRS.getString("Jurisdiction_Currency_Code"));
        String customerContactPoint = tidyString(accountDetailsRS.getString("Customer_Contact_Point"));
        String countryCode = tidyString(accountDetailsRS.getString("Country_Code"));
        customerVATRegNo = tidyString(accountDetailsRS.getString("Customer_VAT_Reg_No"));
        customerVATRegLiteral = tidyString(accountDetailsRS.getString("Customer_VAT_Reg_Literal"));
        customerVATRegText = tidyString(accountDetailsRS.getString("Customer_VAT_Reg_Text"));
        String billClosedDate = tidyString(accountDetailsRS.getString("Bill_Closed_Date"));
        String restrictionType = tidyString(accountDetailsRS.getString("Restriction_Type"));
        String registeredAddress1 = tidyString(accountDetailsRS.getString("Registered_Address1"));
        String registeredAddress2 = tidyString(accountDetailsRS.getString("Registered_Address2"));
        String registeredAddress3 = tidyString(accountDetailsRS.getString("Registered_Address3"));
        String registeredAddress4 = tidyString(accountDetailsRS.getString("Registered_Address4"));
        String registeredAddress5 = tidyString(accountDetailsRS.getString("Registered_Address5"));
        panNumber = tidyString(accountDetailsRS.getString("PAN_Number"));
        // account record
        workLine =
          accountLine(
            bsName,
            workFileNo,
            recordSeqNo,
            globalCustomerId,
            invoiceRegion,
            invoiceNo,
            lastMaintainedDate,
            generalLedger,
            GLCode,
            SAPCustomerNumber,
            outgoingCurrencyCode,
            companyName,
            billingContact,
            billingCustomerName,
            restrictionType,
            customerContactPoint,
            accountReference,
            taxReferenceLiteral,
            taxReference,
            jurisdictionCurrencyCode );
        writeToWorkFile(workLine);
        recordSeqNo++;
        // billing address record
        workLine =
          addressLine(
            bsName,
            workFileNo,
            recordSeqNo,
            globalCustomerId,
            invoiceRegion,
            invoiceNo,
            BILLING,
            lastMaintainedDate,
            billingAddress1,
            billingAddress2,
            billingAddress3,
            billingAddress4,
            billingAddress5,
            billingAddress6,
            billingAddress7,
            EMPTY,
            EMPTY,
          countryCode);
        writeToWorkFile(workLine);
        recordSeqNo++;
        // main bank address record
        workLine =
          bankLine(
            bsName,
            workFileNo,
            recordSeqNo,
            globalCustomerId,
            invoiceRegion,
            invoiceNo,
            MAIN,
            lastMaintainedDate,
            bankAddress1,
            bankAddress2,
            bankAddress3,
            bankAddress4,
            bankAddress5,
            bankAddress6,
            bankAddress7,
            EMPTY,
            EMPTY,
            EMPTY );
        writeToWorkFile(workLine);
        recordSeqNo++;
        // alternate bank address record
        workLine =
          bankLine(
            bsName,
            workFileNo,
            recordSeqNo,
            globalCustomerId,
            invoiceRegion,
            invoiceNo,
            ALTERNATE,
            lastMaintainedDate,
            altBankAddress1,
            altBankAddress2,
            altBankAddress3,
            altBankAddress4,
            altBankAddress5,
            altBankAddress6,
            altBankAddress7,
            EMPTY,
            EMPTY,
            EMPTY );
        writeToWorkFile(workLine);
        recordSeqNo++;
        // registered address record (only produce if it has one)
        if (registeredAddress1.length()>0)
        {
          workLine =
            addressLine(
              bsName,
              workFileNo,
              recordSeqNo,
              globalCustomerId,
              invoiceRegion,
              invoiceNo,
              REGISTERED,
              lastMaintainedDate,
              registeredAddress1,
              registeredAddress2,
              registeredAddress3,
              registeredAddress4,
              registeredAddress5,
              EMPTY,
              EMPTY,
              EMPTY,
              EMPTY,
              EMPTY);
          writeToWorkFile(workLine);
          recordSeqNo++;
        }
        result = true;
      }
    }
    catch(java.sql.SQLException ex)
    {
      message = "   Error getting account details for data invoice "+globalCustomerId+"/"+invoiceNo+" : "+ex.toString();
      System.out.println(message);
      writeToLogFile(message);
    }
    finally
    {
      accountDetailsRS = null;
    }
    return result;
  }

  private boolean dataChargeDetails(String globalCustomerId, String invoiceRegion, String invoiceNo)
  {
    boolean result = true;
    String workLine = "";
    // Determine the Global Customer Division Id and Billing Region combinations
    ResultSet gcdbrsRS = eDB.getDataGCDBRs(globalCustomerId,invoiceNo);
    try
    {
      while (gcdbrsRS.next())
      {
        String globalCustomerDivisionId = tidyString(gcdbrsRS.getString("Global_Customer_Division_Id"));
        String billingRegion = gcdbrsRS.getString("Billing_Region");
        // Get charge line data for the Global Customer Division Id and Billing Region combination
        ResultSet gcdbrLineRS =
          eDB.getDataGCDBRLine(globalCustomerId,invoiceRegion,invoiceNo,globalCustomerDivisionId,billingRegion);
        try
        {
          if (gcdbrLineRS.next())
          {
            String amount = tidyNumericString(gcdbrLineRS.getString("Amount"));
            String outgoingCurrencyCode = tidyString(gcdbrLineRS.getString("Outgoing_Currency_Code"));
            String globalAccountManager = tidyString(gcdbrLineRS.getString("Global_Account_Manager"));
            String reportingRegion = tidyString(gcdbrLineRS.getString("Reporting_Region"));
            String customerContact = tidyString(gcdbrLineRS.getString("Customer_Contact"));
            String globalCustomerName = tidyString(gcdbrLineRS.getString("Global_Customer_Name"));
            String regionId = tidyString(gcdbrLineRS.getString("Region_Id"));
            String grossAmountReference = tidyNumericString(gcdbrLineRS.getString("Gross_Amount_Reference"));
            String netAmountReference = tidyNumericString(gcdbrLineRS.getString("Net_Amount_Reference"));
            String exrateOutgoingToGBP = tidyNumericString(gcdbrLineRS.getString("Exrate_Outgoing_To_GBP"));
            String translationDate = tidyString(gcdbrLineRS.getString("Translation_Date"));
            String discountAmount = tidyNumericString(gcdbrLineRS.getString("Discount_Amount"));
            String serviceDescription = tidyString(gcdbrLineRS.getString("Service_Description"));
            BPSD = tidyString(gcdbrLineRS.getString("BPSD"));
            BPED = tidyString(gcdbrLineRS.getString("BPED"));
            // charge line record
            String workDesc =
              "Total Charges (excluding Tax) "+
              regionId+"/"+globalCustomerDivisionId;
            workLine =
              invoiceLine(
                bsName,
                workFileNo,
                recordSeqNo,
                globalCustomerId,
                invoiceRegion,
                invoiceNo,
                CHARGE,
                lastMaintainedDate,
                amount,
                amount,
                outgoingCurrencyCode,
                outgoingCurrencyCode,
                workDesc,
                ONE,
                ZERO,
                GCD);
            writeToWorkFile(workLine);
            recordSeqNo++;
            // Source data records
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Global_Customer_Division_Id",STRING,"20",EMPTY,globalCustomerDivisionId,EMPTY,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Global_Account_Manager",STRING,"50",EMPTY,globalAccountManager,EMPTY,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Reporting_Region",STRING,"50",EMPTY,reportingRegion,EMPTY,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Customer_Contact",STRING,"200",EMPTY,customerContact,EMPTY,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Global_Customer_Name",STRING,"200",EMPTY,globalCustomerName,EMPTY,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Region_Id",STRING,"20",EMPTY,regionId,EMPTY,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Gross_Amount_Reference",NUMBER,"18","2",EMPTY,grossAmountReference,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Net_Amount_Reference",NUMBER,"18","2",EMPTY,netAmountReference,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "ExRate_Outgoing_To_GBP",NUMBER,"15","7",EMPTY,exrateOutgoingToGBP,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Translation_Date",DATE,EMPTY,EMPTY,EMPTY,EMPTY,translationDate);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Discount_Amount",NUMBER,"22","5",EMPTY,discountAmount,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Customer_VAT_Reg_No",STRING,"30",EMPTY,customerVATRegNo,EMPTY,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Customer_VAT_Reg_Literal",STRING,"50",EMPTY,customerVATRegLiteral,EMPTY,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Customer_VAT_Reg_Text",STRING,"200",EMPTY,customerVATRegText,EMPTY,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Service_Description",STRING,"200",EMPTY,serviceDescription,EMPTY,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "PAN_Number",STRING,"50",EMPTY,panNumber,EMPTY,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
          }
          else
          {
            message =
            "   No gcdbrs line data for invoice "+
            globalCustomerId+"/"+invoiceNo+
            " global customer division id / billing region "+
            globalCustomerDivisionId+"/"+billingRegion;
            System.out.println(message);
            writeToLogFile(message);
            result = false;
          }
        }
        catch(java.sql.SQLException ex)
        {
          message =
            "   Error getting gcdbrs line data for invoice "+
            globalCustomerId+"/"+invoiceNo+
            " global customer division id / billing region "+
            globalCustomerDivisionId+"/"+billingRegion+
            " : "+ex.toString();
          System.out.println(message);
          writeToLogFile(message);
          result = false;
        }
        finally
        {
          gcdbrLineRS = null;
        }
        // Get charge detail data for the global customer division id / billing region combination
        ResultSet gcdbrDetailRS =
          eDB.getDataGCDBRDetail(globalCustomerId,invoiceRegion,invoiceNo,globalCustomerDivisionId,billingRegion);
        try
        {
          while (gcdbrDetailRS.next())
          {
            String billPeriodStartDate = tidyString(gcdbrDetailRS.getString("Bill_Period_Start_Date"));
            String billPeriodEndDate = tidyString(gcdbrDetailRS.getString("Bill_Period_End_Date"));
            String serviceReference = tidyString(gcdbrDetailRS.getString("Service_Reference"));
            String chargeTypeCode = tidyString(gcdbrDetailRS.getString("Charge_Type_Code"));
            String localAmount = tidyNumericString(gcdbrDetailRS.getString("Local_Amount"));
            String amount = tidyNumericString(gcdbrDetailRS.getString("Amount"));
            String notes = tidyString(gcdbrDetailRS.getString("Notes"));
            String regionId = tidyString(gcdbrDetailRS.getString("Region_Id"));
            long invoiceLineNo = gcdbrDetailRS.getLong("Invoice_Line_No");
            String localCurrency = tidyString(gcdbrDetailRS.getString("Local_Currency"));
            String outgoingCurrencyCode = tidyString(gcdbrDetailRS.getString("Outgoing_Currency_Code"));
            String outgoingExchangeRate = tidyNumericString(gcdbrDetailRS.getString("Outgoing_Exchange_Rate"));
            String taxType = tidyString(gcdbrDetailRS.getString("Tax_Type"));
            String globalAccountManager = tidyString(gcdbrDetailRS.getString("Global_Account_Manager"));
            String reportingRegion = tidyString(gcdbrDetailRS.getString("Reporting_Region"));
            String customerContact = tidyString(gcdbrDetailRS.getString("Customer_Contact"));
            String globalCustomerName = tidyString(gcdbrDetailRS.getString("Global_Customer_Name"));
            String grossAmountReference = tidyNumericString(gcdbrDetailRS.getString("Gross_Amount_Reference"));
            String netAmountReference = tidyNumericString(gcdbrDetailRS.getString("Net_Amount_Reference"));
            String billingCessationDate = tidyString(gcdbrDetailRS.getString("Billing_Cessation_Date"));
            String billingStartDate = tidyString(gcdbrDetailRS.getString("Billing_Start_Date"));
            String city = tidyString(gcdbrDetailRS.getString("City"));
            String country = tidyString(gcdbrDetailRS.getString("Country"));
            String customerSite1 = tidyString(gcdbrDetailRS.getString("Customer_Site_1"));
            String customerSite2 = tidyString(gcdbrDetailRS.getString("Customer_Site_2"));
            String discountCurrency = tidyNumericString(gcdbrDetailRS.getString("Discount_Currency"));
            String discountPercent = tidyNumericString(gcdbrDetailRS.getString("Discount_Percent"));
            String discountRegion = tidyString(gcdbrDetailRS.getString("Discount_Region"));
            String discountUSD = tidyNumericString(gcdbrDetailRS.getString("Discount_USD"));
            String fromChargeValidDate = tidyString(gcdbrDetailRS.getString("From_Charge_Valid_Date"));
            String fromEnd = tidyString(gcdbrDetailRS.getString("From_End"));
            String fromEndCode = tidyString(gcdbrDetailRS.getString("From_End_Code"));
            String fromChargeValidDate2 = tidyString(gcdbrDetailRS.getString("FromChargeValidDate"));
            String grossAmountUSD = tidyNumericString(gcdbrDetailRS.getString("Gross_Amount_USD"));
            String liveServiceDate = tidyString(gcdbrDetailRS.getString("Live_Service_Date"));
            long monthlyBillingId = gcdbrDetailRS.getLong("Monthly_Billing_Id");
            String percentage = tidyNumericString(gcdbrDetailRS.getString("Percentage"));
            String productType = tidyString(gcdbrDetailRS.getString("Product_Type"));
            String regionDescription = tidyString(gcdbrDetailRS.getString("Region_Description"));
            String servRefTotalChargeCurr = tidyNumericString(gcdbrDetailRS.getString("Serv_Ref_Total_Charge_Curr"));
            String servRefTotalChargeRef = tidyNumericString(gcdbrDetailRS.getString("Serv_Ref_Total_Charge_Ref"));
            String servRefTotalChargeUSD = tidyNumericString(gcdbrDetailRS.getString("Serv_Ref_Total_Charge_USD"));
            String serviceLevelNotes = tidyString(gcdbrDetailRS.getString("Service_Level_Notes"));
            String site = tidyString(gcdbrDetailRS.getString("Site"));
            String siteAddress = tidyString(gcdbrDetailRS.getString("Site_Address"));
            String siteId = tidyString(gcdbrDetailRS.getString("Site_Id"));
            String siteName = tidyString(gcdbrDetailRS.getString("Site_Name"));
            String sitePercentage = tidyNumericString(gcdbrDetailRS.getString("Site_Percentage"));
            String siteSize = tidyString(gcdbrDetailRS.getString("Site_Size"));
            String speed = tidyString(gcdbrDetailRS.getString("Speed"));
            String TAMSNumber = tidyString(gcdbrDetailRS.getString("TAMS_Number"));
            String taxCurrency = tidyNumericString(gcdbrDetailRS.getString("Tax_Currency"));
            String taxUSD = tidyNumericString(gcdbrDetailRS.getString("Tax_USD"));
            String toChargeValidDate = tidyString(gcdbrDetailRS.getString("To_Charge_Valid_Date"));
            String toEnd = tidyString(gcdbrDetailRS.getString("To_End"));
            String toEndCode = tidyString(gcdbrDetailRS.getString("To_End_Code"));
            String toChargeValidDate2 = tidyString(gcdbrDetailRS.getString("ToChargeValidDate"));
            String exchangeRateToUSD = tidyNumericString(gcdbrDetailRS.getString("Exchange_Rate_To_USD"));
            String BPSDOrig = tidyString(gcdbrDetailRS.getString("BPSD_Orig"));
            String revenueTypeCode = tidyString(gcdbrDetailRS.getString("Revenue_Type_Code"));
            String revenueReasonCode = tidyString(gcdbrDetailRS.getString("Revenue_Reason_Code"));
            String discountAmountLocal = tidyNumericString(gcdbrDetailRS.getString("Discount_Amount_Local"));
            String discountAmount = tidyNumericString(gcdbrDetailRS.getString("Discount_Amount"));
            String revenueNetOrFullCode = tidyString(gcdbrDetailRS.getString("Revenue_Net_Or_Full_Code"));
            String revenueRootCauseCode = tidyString(gcdbrDetailRS.getString("Revenue_Root_Cause_Code"));
            String customerData1 = tidyString(gcdbrDetailRS.getString("Customer_Data_1"));
            String customerData2 = tidyString(gcdbrDetailRS.getString("Customer_Data_2"));
            String chargeDescriptionCode = tidyString(gcdbrDetailRS.getString("Charge_Description_Code"));
            long chargeId = gcdbrDetailRS.getLong("Charge_Id");
            String SGDExRate = tidyNumericString(gcdbrDetailRS.getString("SGD_ExRate"));
            long SGDDecimalPlaces = gcdbrDetailRS.getLong("SGD_Decimal_Places");
            String chargeEntityCode = tidyString(gcdbrDetailRS.getString("Charge_Entity_Code"));
            String chargeType = tidyString(gcdbrDetailRS.getString("Charge_Type"));
            String orderReference = tidyString(gcdbrDetailRS.getString("Order_Reference"));
            String yourRef = tidyString(gcdbrDetailRS.getString("Your_Ref"));
            String quoteNo = tidyString(gcdbrDetailRS.getString("Quote_No"));
            long units = gcdbrDetailRS.getLong("Units");
            long duration = gcdbrDetailRS.getLong("Duration");
            String rate = tidyNumericString(gcdbrDetailRS.getString("Rate"));
            String serviceDescription = tidyString(gcdbrDetailRS.getString("Service_Description"));
            String compositeTax = tidyString(gcdbrDetailRS.getString("Composite_Tax"));
            String mainTaxAmount = tidyNumericString(gcdbrDetailRS.getString("Main_Tax_Amount"));
            String mainTaxType = tidyString(gcdbrDetailRS.getString("Main_Tax_Type"));
            String mainTaxRate = tidyNumericString(gcdbrDetailRS.getString("Main_Tax_Rate"));
            String subTaxAmount1 = tidyNumericString(gcdbrDetailRS.getString("Sub_Tax_Amount_1"));
            String subTaxType1 = tidyString(gcdbrDetailRS.getString("Sub_Tax_Type_1"));
            String subTaxRate1 = tidyNumericString(gcdbrDetailRS.getString("Sub_Tax_Rate_1"));
            String subTaxAmount2 = tidyNumericString(gcdbrDetailRS.getString("Sub_Tax_Amount_2"));
            String subTaxType2 = tidyString(gcdbrDetailRS.getString("Sub_Tax_Type_2"));
            String subTaxRate2 = tidyNumericString(gcdbrDetailRS.getString("Sub_Tax_Rate_2"));
            String compositeTaxRate = tidyNumericString(gcdbrDetailRS.getString("Composite_Tax_Rate"));
            String serviceName = tidyString(gcdbrDetailRS.getString("Service_Name"));
            String c2RefNo = tidyString(gcdbrDetailRS.getString("C2_Ref_No"));
            long paymentDays = gcdbrDetailRS.getLong("Payment_Days");
            // charge detail record
            workLine =
              invoiceDetail(
                bsName,
                workFileNo,
                recordSeqNo,
                globalCustomerId,
                invoiceRegion,
                invoiceNo,
                CHARGE,
                billingStartDate,
                amount,
                localAmount,
                localCurrency,
                outgoingCurrencyCode,
                "Total Charges (excluding Tax)",
                outgoingExchangeRate,
                EMPTY,
                serviceName);
            writeToWorkFile(workLine);
            recordSeqNo++;
            // charge detail source records
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Global_Customer_Division_Id",STRING,"20",EMPTY,globalCustomerDivisionId,EMPTY,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Bill_Period_Start_Date",DATE,EMPTY,EMPTY,EMPTY,EMPTY,billPeriodStartDate);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Bill_Period_End_Date",DATE,EMPTY,EMPTY,EMPTY,EMPTY,billPeriodEndDate);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Service_Reference",STRING,"50",EMPTY,serviceReference,EMPTY,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Charge_Type_Code",STRING,"2",EMPTY,chargeTypeCode,EMPTY,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Notes",STRING,"220",EMPTY,notes,EMPTY,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Region_Id",STRING,"220",EMPTY,regionId,EMPTY,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Invoice_Line_No",NUMBER,"10","0",EMPTY,Long.toString(invoiceLineNo),EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Tax_Type",STRING,"2",EMPTY,taxType,EMPTY,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Global_Account_Manager",STRING,"50",EMPTY,globalAccountManager,EMPTY,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Reporting_Region",STRING,"50",EMPTY,reportingRegion,EMPTY,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Customer_Contact",STRING,"200",EMPTY,customerContact,EMPTY,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Global_Customer_Name",STRING,"100",EMPTY,globalCustomerName,EMPTY,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Billing_Region",STRING,"100",EMPTY,billingRegion.trim(),EMPTY,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Gross_Amount_Reference",NUMBER,"18","2",EMPTY,grossAmountReference,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Net_Amount_Reference",NUMBER,"18","2",EMPTY,netAmountReference,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Billing_Cessation_Date",DATE,EMPTY,EMPTY,EMPTY,EMPTY,billingCessationDate);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Billing_Start_Date",DATE,EMPTY,EMPTY,EMPTY,EMPTY,billingStartDate);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "City",STRING,"30",EMPTY,city,EMPTY,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Country",STRING,"30",EMPTY,country,EMPTY,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Customer_Site_1",STRING,"50",EMPTY,customerSite1,EMPTY,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Customer_Site_2",STRING,"50",EMPTY,customerSite2,EMPTY,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Discount_Currency",NUMBER,"18","2",EMPTY,discountCurrency,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Discount_Percent",NUMBER,"5","2",EMPTY,discountPercent,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Discount_Region",STRING,"50",EMPTY,discountRegion,EMPTY,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Discount_USD",NUMBER,"18","2",EMPTY,discountUSD,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "From_Charge_Valid_Date",DATE,EMPTY,EMPTY,EMPTY,EMPTY,fromChargeValidDate);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "From_End",STRING,"200",EMPTY,fromEnd,EMPTY,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "From_End_Code",STRING,"200",EMPTY,fromEndCode,EMPTY,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "FromChargeValidDate",DATE,EMPTY,EMPTY,EMPTY,EMPTY,fromChargeValidDate2);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Gross_Amount_USD",NUMBER,"18","2",EMPTY,grossAmountUSD,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Live_Service_Date",DATE,EMPTY,EMPTY,EMPTY,EMPTY,liveServiceDate);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Monthly_Billing_Id",NUMBER,"22","0",EMPTY,Long.toString(monthlyBillingId),EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "percentage",NUMBER,"5","5",EMPTY,percentage,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Product_Type",STRING,"50",EMPTY,productType,EMPTY,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Region_Description",STRING,"50",EMPTY,regionDescription,EMPTY,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Serv_Ref_Total_Charge_Curr",NUMBER,"18","2",EMPTY,servRefTotalChargeCurr,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Serv_Ref_Total_Charge_Ref",NUMBER,"18","2",EMPTY,servRefTotalChargeRef,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Serv_Ref_Total_Charge_USD",NUMBER,"18","2",EMPTY,servRefTotalChargeUSD,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Service_Level_Notes",STRING,"220",EMPTY,serviceLevelNotes,EMPTY,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Site",STRING,"50",EMPTY,site,EMPTY,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Site_Address",STRING,"255",EMPTY,siteAddress,EMPTY,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Site_Id",STRING,"50",EMPTY,siteId,EMPTY,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Site_Name",STRING,"100",EMPTY,siteName,EMPTY,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Site_Percentage",NUMBER,"20","2",EMPTY,sitePercentage,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Site_Size",STRING,"20",EMPTY,siteSize,EMPTY,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Speed",STRING,"20",EMPTY,speed,EMPTY,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "TAMS_Number",STRING,"100",EMPTY,TAMSNumber,EMPTY,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Tax_Currency",NUMBER,"18","2",EMPTY,taxCurrency,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Tax_USD",NUMBER,"18","2",EMPTY,taxUSD,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "To_Charge_Valid_Date",DATE,EMPTY,EMPTY,EMPTY,EMPTY,toChargeValidDate);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "To_End",STRING,"200",EMPTY,toEnd,EMPTY,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "To_End_Code",STRING,"200",EMPTY,toEndCode,EMPTY,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "ToChargeValidDate",DATE,EMPTY,EMPTY,EMPTY,EMPTY,toChargeValidDate2);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Exchange_Rate_To_USD",NUMBER,"15","7",EMPTY,exchangeRateToUSD,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "BPSD_Orig",DATE,EMPTY,EMPTY,EMPTY,EMPTY,BPSDOrig);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Revenue_Type_Code",STRING,"5",EMPTY,revenueTypeCode,EMPTY,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Revenue_Reason_Code",STRING,"5",EMPTY,revenueReasonCode,EMPTY,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Discount_Amount",NUMBER,"22","5",EMPTY,discountAmount,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Discount_Amount_Local",NUMBER,"22","5",EMPTY,discountAmountLocal,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Revenue_Net_or_Full_Code",STRING,"5",EMPTY,revenueNetOrFullCode,EMPTY,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Revenue_Root_Cause_Code",STRING,"5",EMPTY,revenueRootCauseCode,EMPTY,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Customer_Data_1",STRING,"100",EMPTY,customerData1,EMPTY,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Customer_Data_2",STRING,"100",EMPTY,customerData2,EMPTY,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Charge_Description_Code",STRING,"50",EMPTY,chargeDescriptionCode,EMPTY,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Charge_Id",NUMBER,"38","0",EMPTY,Long.toString(chargeId),EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "SGD_ExRate",NUMBER,"15","7",EMPTY,SGDExRate,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "SGD_Decimal_Places",NUMBER,"38","0",EMPTY,Long.toString(SGDDecimalPlaces),EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Charge_Entity_Code",STRING,"10",EMPTY,chargeEntityCode,EMPTY,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Charge_Type",STRING,"20",EMPTY,chargeType,EMPTY,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Order_Reference",STRING,"50",EMPTY,orderReference,EMPTY,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Your_Ref",STRING,"50",EMPTY,yourRef,EMPTY,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Quote_No",STRING,"50",EMPTY,quoteNo,EMPTY,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Units",NUMBER,"18",EMPTY,EMPTY,Long.toString(units),EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Duration",NUMBER,"18",EMPTY,EMPTY,Long.toString(duration),EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Rate",NUMBER,"15","7",EMPTY,rate,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Service_Description",STRING,"255",EMPTY,serviceDescription,EMPTY,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Composite_Tax",STRING,"1",EMPTY,compositeTax,EMPTY,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Main_Tax_Amount",NUMBER,"20","5",EMPTY,mainTaxAmount,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Main_Tax_Type",STRING,"3",EMPTY,mainTaxType,EMPTY,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Main_Tax_Rate",NUMBER,"15","7",EMPTY,mainTaxRate,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Sub_Tax_Amount_1",NUMBER,"20","5",EMPTY,subTaxAmount1,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Sub_Tax_Type_1",STRING,"3",EMPTY,subTaxType1,EMPTY,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Sub_Tax_Rate_1",NUMBER,"15","7",EMPTY,subTaxRate1,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Sub_Tax_Amount_2",NUMBER,"20","5",EMPTY,subTaxAmount2,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Sub_Tax_Type_2",STRING,"3",EMPTY,subTaxType2,EMPTY,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Sub_Tax_Rate_2",NUMBER,"15","7",EMPTY,subTaxRate2,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Composite_Tax_Rate",NUMBER,"15","7",EMPTY,compositeTaxRate,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "C2_Ref_No",STRING,"50",EMPTY,c2RefNo,EMPTY,EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                          "Payment_Days",NUMBER,"38","0",EMPTY,Long.toString(paymentDays),EMPTY);
            writeToWorkFile(workLine);
            recordSeqNo++;
          }
        }
        catch(java.sql.SQLException ex)
        {
          message =
            "   Error getting gcdbrs detail data for invoice "+
            globalCustomerId+"/"+invoiceNo+
            " global customer division id / billing region "+
            globalCustomerDivisionId+"/"+billingRegion+
            " : "+ex.toString();
          System.out.println(message);
          writeToLogFile(message);
          result = false;
        }
        finally
        {
          gcdbrDetailRS = null;
        }
      }
    }
    catch(java.sql.SQLException ex)
    {
      message = "   Error getting gcdbrs for data invoice "+globalCustomerId+"/"+invoiceNo+" : "+ex.toString();
      System.out.println(message);
      writeToLogFile(message);
      result = false;
    }
    finally
    {
      gcdbrsRS = null;
    }
    return result;
  }

  private boolean dataTaxDetails(String globalCustomerId, String invoiceRegion, String invoiceNo)
  {
    boolean result = true;
    String workLine = "";
    // get tax line data
    ResultSet taxLineRS = eDB.getDataTaxLine(globalCustomerId,invoiceRegion,invoiceNo);
    try
    {
      if (taxLineRS.next())
      {
        String taxAmount = tidyNumericString(taxLineRS.getString("Tax_Amount"));
        String outgoingCurrencyCode = tidyString(taxLineRS.getString("Outgoing_Currency_Code"));
        String jurisdictionExRate = tidyNumericString(taxLineRS.getString("Jurisdiction_ExRate"));
        String SAPTaxCode = tidyString(taxLineRS.getString("SAP_Tax_Code"));
        String taxType = tidyString(taxLineRS.getString("Tax_Type"));
        // invoice tax line
        workLine =
          invoiceLine(
            bsName,
            workFileNo,
            recordSeqNo,
            globalCustomerId,
            invoiceRegion,
            invoiceNo,
            TAX,
            lastMaintainedDate,
            taxAmount,
            taxAmount,
            outgoingCurrencyCode,
            outgoingCurrencyCode,
            "Tax in "+outgoingCurrencyCode,
            "1.0",
            EMPTY,
            GCD);
        writeToWorkFile(workLine);
        recordSeqNo++;
        // tax line source data
        workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                      "Jurisdiction_ExRate",NUMBER,"15","7",EMPTY,jurisdictionExRate,EMPTY);
        writeToWorkFile(workLine);
        recordSeqNo++;
        workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                      "SAP_Tax_Code",STRING,"5",EMPTY,SAPTaxCode,EMPTY,EMPTY);
        writeToWorkFile(workLine);
        recordSeqNo++;
        workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                      "Tax_Type",STRING,"5",EMPTY,taxType,EMPTY,EMPTY);
        writeToWorkFile(workLine);
        recordSeqNo++;
      }
    }
    catch(java.sql.SQLException ex)
    {
      message = "   Error getting tax line for data invoice "+globalCustomerId+"/"+invoiceNo+" : "+ex.toString();
      System.out.println(message);
      writeToLogFile(message);
      result = false;
    }
    finally
    {
      taxLineRS = null;
    }
    // get tp detail data
    ResultSet tpDetRS = eDB.getDataTPDet(globalCustomerId,invoiceNo);
    try
    {
      while (tpDetRS.next())
      {
        String taxAmount = tidyNumericString(tpDetRS.getString("Tax_Amount"));
        String taxRate = tidyNumericString(tpDetRS.getString("Tax_Rate"));
        String billingRegion = tidyString(tpDetRS.getString("Billing_Region"));
        String taxType = tidyString(tpDetRS.getString("Tax_Type"));
        String billingPeriodStartDate = tidyString(tpDetRS.getString("Billing_Period_Start_Date"));
        String outgoingCurrencyCode = tidyString(tpDetRS.getString("Outgoing_Currency_Code"));
        String taxTypeDescription = tidyString(tpDetRS.getString("Tax_Type_Description"));
        String additionalTaxRate = tidyNumericString(tpDetRS.getString("Additional_Tax_Rate"));
        // TP detail line
        workLine =
          invoiceDetail(
            bsName,
            workFileNo,
            recordSeqNo,
            globalCustomerId,
            invoiceRegion,
            invoiceNo,
            TAX,
            billingPeriodStartDate,
            taxAmount,
            taxAmount,
            outgoingCurrencyCode,
            outgoingCurrencyCode,
            "Monthly Charge Tax",
            "1.0",
            taxRate,
            taxTypeDescription);
        writeToWorkFile(workLine);
        recordSeqNo++;
        // tp source data
        workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                      "Billing_Region",STRING,"100",EMPTY,billingRegion,EMPTY,EMPTY);
        writeToWorkFile(workLine);
        recordSeqNo++;
        workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                      "Tax_Type",STRING,"5",EMPTY,taxType,EMPTY,EMPTY);
        writeToWorkFile(workLine);
        recordSeqNo++;
        workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                      "Additional_Tax_Rate",NUMBER,"15","7",EMPTY,additionalTaxRate,EMPTY);
        writeToWorkFile(workLine);
        recordSeqNo++;
      }
    }
    catch(java.sql.SQLException ex)
    {
      message = "   Error getting tp detail for data invoice "+globalCustomerId+"/"+invoiceNo+" : "+ex.toString();
      System.out.println(message);
      writeToLogFile(message);
      result = false;
    }
    finally
    {
      tpDetRS = null;
    }
    // get vertex detail data
    ResultSet vertexDetRS = eDB.getDataVertexDet(globalCustomerId,invoiceNo);
    try
    {
      while (vertexDetRS.next())
      {
        String taxAmount = tidyNumericString(vertexDetRS.getString("Tax_Amount"));
        String taxAuthority = tidyString(vertexDetRS.getString("Tax_Authority"));
        String outgoingCurrencyCode = tidyString(vertexDetRS.getString("Outgoing_Currency_Code"));
        String taxTypeDescription = tidyString(vertexDetRS.getString("Tax_Type_Description"));
        long monthlyBillingId = vertexDetRS.getLong("Monthly_Billing_Id");
        String mbtTaxType = tidyString(vertexDetRS.getString("mbt_Tax_Type"));
        String taxAuthorityDescription = tidyString(vertexDetRS.getString("Tax_Authority_Description"));
        String taxTypeCode = tidyString(vertexDetRS.getString("Tax_Type_Code"));
        String taxTypeDesc = tidyString(vertexDetRS.getString("Tax_Type_Desc"));
        String geocode = tidyString(vertexDetRS.getString("Geocode"));
        String stateName = tidyString(vertexDetRS.getString("State_Name"));
        // vertex tax detail line
        workLine =
          invoiceDetail(
            bsName,
            workFileNo,
            recordSeqNo,
            globalCustomerId,
            invoiceRegion,
            invoiceNo,
            TAX,
            lastMaintainedDate,
            taxAmount,
            taxAmount,
            outgoingCurrencyCode,
            outgoingCurrencyCode,
            "Monthly Charge Tax",
            "1.0",
            ZERO,
            taxTypeDescription);
        writeToWorkFile(workLine);
        recordSeqNo++;
        // vertex tax detail source data
        workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                      "Tax_Authority",STRING,"2",EMPTY,taxAuthority,EMPTY,EMPTY);
        writeToWorkFile(workLine);
        recordSeqNo++;
        workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                      "Tax_Type",STRING,"5",EMPTY,EMPTY,EMPTY,EMPTY);
        writeToWorkFile(workLine);
        recordSeqNo++;
        workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                      "mbt_Tax_Type",STRING,"2",EMPTY,mbtTaxType,EMPTY,EMPTY);
        writeToWorkFile(workLine);
        recordSeqNo++;
        workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                      "Tax_Authority_Description",STRING,"255",EMPTY,taxAuthorityDescription,EMPTY,EMPTY);
        writeToWorkFile(workLine);
        recordSeqNo++;
        workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                      "Tax_Type_Code",STRING,"2",EMPTY,taxTypeCode,EMPTY,EMPTY);
        writeToWorkFile(workLine);
        recordSeqNo++;
        workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                      "Tax_Type_Description",STRING,"2",EMPTY,taxTypeDesc,EMPTY,EMPTY);
        writeToWorkFile(workLine);
        recordSeqNo++;
        workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                      "Monthly_Billing_Id",NUMBER,"38","0",EMPTY,Long.toString(monthlyBillingId),EMPTY);
        writeToWorkFile(workLine);
        recordSeqNo++;
        workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                      "Geocode",STRING,"20",EMPTY,geocode,EMPTY,EMPTY);
        writeToWorkFile(workLine);
        recordSeqNo++;
        workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                      "State_Name",STRING,"50",EMPTY,stateName,EMPTY,EMPTY);
        writeToWorkFile(workLine);
        recordSeqNo++;
      }
    }
    catch(java.sql.SQLException ex)
    {
      message = "   Error getting vertex detail for data invoice "+globalCustomerId+"/"+invoiceNo+" : "+ex.toString();
      System.out.println(message);
      writeToLogFile(message);
      result = false;
    }
    finally
    {
      vertexDetRS = null;
    }
    // get adjustment tax detail data
    ResultSet adjtaxDetRS = eDB.getDataAdjTaxDet(globalCustomerId,invoiceNo);
    try
    {
      while (adjtaxDetRS.next())
      {
        String taxAmount = tidyNumericString(adjtaxDetRS.getString("Tax_Amount"));
        String taxRate = tidyNumericString(adjtaxDetRS.getString("Tax_Rate"));
        String taxType = adjtaxDetRS.getString("Tax_Type");
        String adjustmentDate = tidyString(adjtaxDetRS.getString("Adjustment_Date"));
        String outgoingCurrencyCode = tidyString(adjtaxDetRS.getString("Outgoing_Currency_Code"));
        String taxTypeDescription = tidyString(adjtaxDetRS.getString("Tax_Type_Description"));
        // adjustment tax detail line
        workLine =
          invoiceDetail(
            bsName,
            workFileNo,
            recordSeqNo,
            globalCustomerId,
            invoiceRegion,
            invoiceNo,
            TAX,
            adjustmentDate,
            taxAmount,
            taxAmount,
            outgoingCurrencyCode,
            outgoingCurrencyCode,
            "Monthly Charge Tax",
            "1.0",
            taxRate,
            "Adjustment Tax");
        writeToWorkFile(workLine);
        recordSeqNo++;
        // adjustment tax detail source data
        workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                      "Tax_Type",STRING,"5",EMPTY,taxType,EMPTY,EMPTY);
        writeToWorkFile(workLine);
        recordSeqNo++;
      }
    }
    catch(java.sql.SQLException ex)
    {
      message = "   Error getting adjustment tax detail for data invoice "+globalCustomerId+"/"+invoiceNo+" : "+ex.toString();
      System.out.println(message);
      writeToLogFile(message);
      result = false;
    }
    finally
    {
      adjtaxDetRS = null;
    }
    return result;
  }

  private boolean dataAdjustmentDetails(String globalCustomerId, String invoiceRegion, String invoiceNo)
  {
    boolean result = true;
    String workLine = "";
    // get adjustment line data
    ResultSet adjDetRS = eDB.getDataAdjDet(globalCustomerId,invoiceNo);
    try
    {
      while (adjDetRS.next())
      {
        String adjustmentAmount = tidyNumericString(adjDetRS.getString("Adjustment_Amount"));
        String adjustmentDescription = tidyString(adjDetRS.getString("Adjustment_Description"));
        String adjustmentDate = tidyString(adjDetRS.getString("Adjustment_Date"));
        String adjustmentTaxAmount = tidyNumericString(adjDetRS.getString("Adjustment_Tax_Amount"));
        String outgoingCurrencyCode = tidyString(adjDetRS.getString("Outgoing_Currency_Code"));
        String adjustmentReference = tidyString(adjDetRS.getString("Adjustment_Reference"));
        String adjustmentDescriptionCode = tidyString(adjDetRS.getString("Adjustment_Description_Code"));
        String adjustmentTypeCode = tidyString(adjDetRS.getString("Adjustment_Type_Code"));
        String revenueNetOrFullCode = tidyString(adjDetRS.getString("Revenue_Net_or_Full_Code"));
        String revenueRootCauseCode = tidyString(adjDetRS.getString("Revenue_Root_Cause_Code"));
        String revenueTypeCode = tidyString(adjDetRS.getString("Revenue_Type_Code"));
        String revenueReasonCode = tidyString(adjDetRS.getString("Revenue_Reason_Code"));
        String chargeEntityCode = tidyString(adjDetRS.getString("Charge_Entity_Code"));
        String PORef = tidyString(adjDetRS.getString("PO_Ref"));
        // invoice tax line
        workLine =
          invoiceLine(
            bsName,
            workFileNo,
            recordSeqNo,
            globalCustomerId,
            invoiceRegion,
            invoiceNo,
            ADJUSTMENT,
            adjustmentDate,
            adjustmentAmount,
            adjustmentAmount,
            outgoingCurrencyCode,
            outgoingCurrencyCode,
            adjustmentDescription,
            "1.0",
            EMPTY,
            GCD);
        writeToWorkFile(workLine);
        recordSeqNo++;
        // adjustment line source data
        workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                      "Adjustment_Tax_Amount",NUMBER,"18","2",EMPTY,adjustmentTaxAmount,EMPTY);
        writeToWorkFile(workLine);
        recordSeqNo++;
        workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                      "Adjustment_Reference",STRING,"50",EMPTY,adjustmentReference,EMPTY,EMPTY);
        writeToWorkFile(workLine);
        recordSeqNo++;
        workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                      "Adjustment_Description_Code",STRING,"5",EMPTY,adjustmentDescriptionCode,EMPTY,EMPTY);
        writeToWorkFile(workLine);
        recordSeqNo++;
        workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                      "Adjustment_Type_Code",STRING,"5",EMPTY,adjustmentTypeCode,EMPTY,EMPTY);
        writeToWorkFile(workLine);
        recordSeqNo++;
        workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                      "Revenue_Net_or_Full_Code",STRING,"5",EMPTY,revenueNetOrFullCode,EMPTY,EMPTY);
        writeToWorkFile(workLine);
        recordSeqNo++;
        workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                      "Revenue_Root_Cause_Code",STRING,"5",EMPTY,revenueRootCauseCode,EMPTY,EMPTY);
        writeToWorkFile(workLine);
        recordSeqNo++;
        workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                      "Revenue_Reason_Code",STRING,"5",EMPTY,revenueReasonCode,EMPTY,EMPTY);
        writeToWorkFile(workLine);
        recordSeqNo++;
        workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                      "Revenue_Type_Code",STRING,"5",EMPTY,revenueTypeCode,EMPTY,EMPTY);
        writeToWorkFile(workLine);
        recordSeqNo++;
        workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                      "Charge_Entity_Code",STRING,"30",EMPTY,chargeEntityCode,EMPTY,EMPTY);
        writeToWorkFile(workLine);
        recordSeqNo++;
        workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                      "PO_Ref",STRING,"30",EMPTY,PORef,EMPTY,EMPTY);
        writeToWorkFile(workLine);
        recordSeqNo++;
      }
    }
    catch(java.sql.SQLException ex)
    {
      message = "   Error getting adjustment detail for data invoice "+globalCustomerId+"/"+invoiceNo+" : "+ex.toString();
      System.out.println(message);
      writeToLogFile(message);
      result = false;
    }
    finally
    {
      adjDetRS = null;
    }
    return result;
  }

  private boolean dataDiscountDetails(String globalCustomerId, String invoiceRegion, String invoiceNo)
  {
    boolean result = true;
    boolean first = true;
    String workLine = "";
    // get discount detail data
    ResultSet discDetRS = eDB.getDataDiscDet(globalCustomerId,invoiceNo);
    try
    {
      while (discDetRS.next())
      {
        String billPeriodStartDate = tidyString(discDetRS.getString("Bill_Period_Start_Date"));
        String accountId = tidyString(discDetRS.getString("Account_Id"));
        String billingRegion = tidyString(discDetRS.getString("Billing_Region"));
        String discountAmountLocal = tidyNumericString(discDetRS.getString("Discount_Amount_Local"));
        String discountAmount = tidyNumericString(discDetRS.getString("Discount_Amount"));
        long discountId = discDetRS.getLong("Discount_Id");
        String discountCurrencyCode = tidyString(discDetRS.getString("Discount_Currency_Code"));
        String outgoingExchangeRate = tidyNumericString(discDetRS.getString("Outgoing_Exchange_Rate"));
        String discountType = tidyString(discDetRS.getString("Discount_Type"));
        String discountPercentage = tidyNumericString(discDetRS.getString("Discount_Percentage"));
        String discountDescription = tidyString(discDetRS.getString("Discount_Description"));
        long discountSequence = discDetRS.getLong("Discount_Sequence");
        String chargeAmountLocal = tidyNumericString(discDetRS.getString("Charge_Amount_Local"));
        String chargeAmount = tidyNumericString(discDetRS.getString("Charge_Amount"));
        String outgoingCurrencyCode = tidyString(discDetRS.getString("Outgoing_Currency_Code"));
        String serviceReference = tidyString(discDetRS.getString("Service_Reference"));
        String notes = tidyString(discDetRS.getString("Notes"));
        String BPSD = tidyString(discDetRS.getString("BPSD"));
        String BPED = tidyString(discDetRS.getString("BPED"));
        String BSD = tidyString(discDetRS.getString("BSD"));
        String globalCustomerName = tidyString(discDetRS.getString("Global_Customer_Name"));
        String regionId = tidyString(discDetRS.getString("Region_Id"));
        String globalAccountManager = tidyString(discDetRS.getString("Global_Account_Manager"));
        String globalCustomerDivisionId = tidyString(discDetRS.getString("Global_Customer_Division_Id"));
        // discounts data dummy supplementary line (only write once)
        if (first)
        {
          workLine =
            supplementaryLine(
              bsName,
              workFileNo,
              recordSeqNo,
              globalCustomerId,
              invoiceRegion,
              invoiceNo,
              CHARGE,
              lastMaintainedDate,
              "0.00",
              "0.00",
              outgoingCurrencyCode,
              outgoingCurrencyCode,
              "Discounts_Detail",
              "1.00",
              EMPTY,
              GCD);
          writeToWorkFile(workLine);
          recordSeqNo++;
          first = false;
        }
        // discounts data detail line
        workLine =
          supplementaryDetail(
            bsName,
            workFileNo,
            recordSeqNo,
            globalCustomerId,
            invoiceRegion,
            invoiceNo,
            CHARGE,
            lastMaintainedDate,
            "0.00",
            "0.00",
            outgoingCurrencyCode,
            outgoingCurrencyCode,
            "Discounts_Detail",
            "1.00",
            EMPTY,
            "One-Off Charge");
        writeToWorkFile(workLine);
        recordSeqNo++;
        // discounts detail source data
        workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                      "Global_Customer_Division_Id",STRING,"50",EMPTY,globalCustomerDivisionId,EMPTY,EMPTY);
        writeToWorkFile(workLine);
        recordSeqNo++;
        workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                      "Global_Customer_Id",STRING,"50",EMPTY,globalCustomerId,EMPTY,EMPTY);
        writeToWorkFile(workLine);
        recordSeqNo++;
        workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                      "Bill_Period_Start_Date",DATE,EMPTY,EMPTY,EMPTY,EMPTY,billPeriodStartDate);
        writeToWorkFile(workLine);
        recordSeqNo++;
        workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                      "Account_Id",STRING,"10",EMPTY,accountId,EMPTY,EMPTY);
        writeToWorkFile(workLine);
        recordSeqNo++;
        workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                      "Billing_Region",STRING,"100",EMPTY,billingRegion,EMPTY,EMPTY);
        writeToWorkFile(workLine);
        recordSeqNo++;
        workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                      "Discount_Amount",NUMBER,"22","5",EMPTY,discountAmount,EMPTY);
        writeToWorkFile(workLine);
        recordSeqNo++;
        workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                      "Discount_Amount_Local",NUMBER,"22","5",EMPTY,discountAmountLocal,EMPTY);
        writeToWorkFile(workLine);
        recordSeqNo++;
        workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                      "Discount_Id",NUMBER,"38","0",EMPTY,Long.toString(discountId),EMPTY);
        writeToWorkFile(workLine);
        recordSeqNo++;
        workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                      "Discount_Currency_Code",STRING,"3",EMPTY,discountCurrencyCode,EMPTY,EMPTY);
        writeToWorkFile(workLine);
        recordSeqNo++;
        workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                      "Invoice_No",STRING,"20",EMPTY,invoiceNo,EMPTY,EMPTY);
        writeToWorkFile(workLine);
        recordSeqNo++;
        workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                      "Outgoing_Exchange_Rate",NUMBER,"18","5",EMPTY,outgoingExchangeRate,EMPTY);
        writeToWorkFile(workLine);
        recordSeqNo++;
        workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                      "Discount_Type",STRING,"5",EMPTY,discountType,EMPTY,EMPTY);
        writeToWorkFile(workLine);
        recordSeqNo++;
        workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                      "Discount_Percentage",NUMBER,"5","2",EMPTY,discountPercentage,EMPTY);
        writeToWorkFile(workLine);
        recordSeqNo++;
        workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                      "Discount_Description",STRING,"100",EMPTY,discountDescription,EMPTY,EMPTY);
        writeToWorkFile(workLine);
        recordSeqNo++;
        workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                      "Discount_Sequence",NUMBER,"5","2",EMPTY,Long.toString(discountSequence),EMPTY);
        writeToWorkFile(workLine);
        workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                      "Charge_Amount",NUMBER,"22","5",EMPTY,chargeAmount,EMPTY);
        writeToWorkFile(workLine);
        recordSeqNo++;
        workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                      "Charge_Amount_Local",NUMBER,"22","5",EMPTY,chargeAmountLocal,EMPTY);
        writeToWorkFile(workLine);
        recordSeqNo++;
        workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                      "Outgoing_Currency_Code",STRING,"3",EMPTY,outgoingCurrencyCode,EMPTY,EMPTY);
        writeToWorkFile(workLine);
        recordSeqNo++;
        workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                      "Service_Reference",STRING,"50",EMPTY,serviceReference,EMPTY,EMPTY);
        writeToWorkFile(workLine);
        recordSeqNo++;
        workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                      "Notes",STRING,"220",EMPTY,notes,EMPTY,EMPTY);
        writeToWorkFile(workLine);
        recordSeqNo++;
        workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                      "BPSD",DATE,EMPTY,EMPTY,EMPTY,EMPTY,BPSD);
        writeToWorkFile(workLine);
        recordSeqNo++;
        workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                      "BPED",DATE,EMPTY,EMPTY,EMPTY,EMPTY,BPED);
        writeToWorkFile(workLine);
        recordSeqNo++;
        workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                      "BSD",DATE,EMPTY,EMPTY,EMPTY,EMPTY,BSD);
        writeToWorkFile(workLine);
        recordSeqNo++;
        workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                      "Global_Customer_Name",STRING,"100",EMPTY,globalCustomerName,EMPTY,EMPTY);
        writeToWorkFile(workLine);
        recordSeqNo++;
        workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                      "Invoice_Region",STRING,"50",EMPTY,invoiceRegion,EMPTY,EMPTY);
        writeToWorkFile(workLine);
        recordSeqNo++;
        workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                      "Region_id",STRING,"50",EMPTY,regionId,EMPTY,EMPTY);
        writeToWorkFile(workLine);
        recordSeqNo++;
        workLine = sourceData(bsName,workFileNo,recordSeqNo,globalCustomerId,invoiceRegion,invoiceNo,
                      "Global_Account_Manager",STRING,"500",EMPTY,globalAccountManager,EMPTY,EMPTY);
        writeToWorkFile(workLine);
        recordSeqNo++;
      }
    }
    catch(java.sql.SQLException ex)
    {
      message = "   Error getting discount details for data invoice "+globalCustomerId+"/"+invoiceNo+" : "+ex.toString();
      System.out.println(message);
      writeToLogFile(message);
      result = false;
    }
    finally
    {
      discDetRS = null;
    }
    return result;
  }

  private boolean dataInvoiceTotal(
    String globalCustomerId,
    String invoiceRegion,
    String invoiceNo )
  {
    boolean result = false;
    boolean totalFound = false;
    String invoiceTotal = ZERO;
    ResultSet invoiceTotalRS = eDB.getDataInvoiceTotal(globalCustomerId,invoiceNo);
    try
    {
      if (invoiceTotalRS.next())
      {
        invoiceTotal = tidyNumericString(invoiceTotalRS.getString("Invoice_Total"));
        totalFound = true;
      }
    }
    catch(java.sql.SQLException ex)
    {
      message = "   Error getting invoice total for data invoice "+globalCustomerId+"/"+invoiceNo+" : "+ex.toString();
      System.out.println(message);
      writeToLogFile(message);
    }
    finally
    {
      invoiceTotalRS = null;
    }
    if (totalFound)
    {
      String workLine =
        invoiceTotalLine(
          bsName,
          workFileNo,
          invoiceRecordSeqNo,
          globalCustomerId,
          invoiceRegion,
          invoiceNo,
          lastMaintainedDate,
          lastMaintainedDate,
          BPSD,
          BPED,
          invoiceTotal,
          GCBDATA );
      writeToWorkFile(workLine);
      result = true;
    }
    return result;
  }

  // open log file
  private void openLogFile()
  {
    logBW = null;
    String logDate = new java.util.Date().toString();
    String logFilename
      = logDir+File.separator+"ebillfeed_"+
        logDate.substring(24,28)+decodeMonth(logDate.substring(4,7))+
        logDate.substring(8,10)+"_"+logDate.substring(11,13)+
        logDate.substring(14,16)+logDate.substring(17,19)+
        "_log.txt";
    try
    {
      logBW = new BufferedWriter(new FileWriter(logFilename));
    }
    catch (IOException e)
    {
      System.out.println("   Error opening log file : "+e.toString());
    }
  }

  // write line to log file
  private void writeToLogFile( String line )
  {
    try
    {
      logBW.write(line+"\r\n");
    }
    catch (IOException e)
    {
      System.out.println("   Error writing to log file : "+e.toString());
    }
  }

  // close log file
  private void closeLogFile()
  {
    try
    {
      logBW.close();
    }
    catch (IOException e)
    {
      System.out.println("   Error closing log file : "+e.toString());
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

  // returns current date/time in dd/mm/yyyy hh:mi:ss format
  private String currentDT()
  {
    String DateTime = new java.util.Date().toString();
    String reformatDT =
      decodeMonth(DateTime.substring(4,7))+"/"+
      DateTime.substring(8,10)+"/"+
      DateTime.substring(24,28)+" "
      +DateTime.substring(11,13)+":"+
      DateTime.substring(14,16)+":"+
      DateTime.substring(17,19);
    return reformatDT;
  }

// returns current date/time in dd/mm/yyyy hh:mi:ss format
  private String creationDT()
  {
    String DateTime = new java.util.Date().toString();
    String reformatDT =
      DateTime.substring(24,28)+"-"+
      decodeMonth(DateTime.substring(4,7))+"-"+
      DateTime.substring(8,10)+" "+
      DateTime.substring(11,13)+":"+
      DateTime.substring(14,16)+":"+
      DateTime.substring(17,19)+".000";
    return reformatDT;
  }

  // returns current date/time in yyyymmddhhmiss format
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

  // returns raw date/time in dd/mm/yyyy hh:mi:ss format
  private String reformatConglomDate(String conglomDate)
  {
    String reformatDate = "";
    if (conglomDate!=null)
    {
      reformatDate =
        conglomDate.substring(24,28)+HYPHEN+
        decodeMonth(conglomDate.substring(4,27))+HYPHEN+
        conglomDate.substring(8,10)+SPACE+
        conglomDate.substring(11,19)+DATEMS;
    }
    return reformatDate;
  }

  private String getDBName(String billingSource)
  {
    String dbName = "";
    if (billingSource.startsWith(DATA))
      dbName = GCD;
    else if (billingSource.startsWith(CONGLOM))
      dbName = CONGLOMERATE;
    else if (billingSource.startsWith(GDIAL))
      dbName = DIALUPPRODUCTS;
    return dbName;
  }

  private String getBSName(String billingSource)
  {
    String bsName = "";
    if (billingSource.startsWith(DATA))
      bsName = GCBDATA;
    else if (billingSource.startsWith(CONGLOM))
      bsName = CONGLOM;
    else if (billingSource.startsWith(GDIAL))
      bsName = GLOBALDIAL;
    return bsName;
  }

  // delete any files in the work directory
  private void clearWorkDirectory()
  {
    File workFolder = new File(workDir);
    File[] workFileArray = workFolder.listFiles();
    for(int i=0;i<workFileArray.length;i++)
    {
      File workFile = workFileArray[i];
      workFile.delete();
    }
  }

  // delete any files in the work2 directory
  private void clearWork2Directory()
  {
    File workFolder = new File(workDir2);
    File[] workFileArray = workFolder.listFiles();
    for(int i=0;i<workFileArray.length;i++)
    {
      File workFile = workFileArray[i];
      workFile.delete();
    }
  }

  // open work file
  private void openWorkFile(String wfName)
  {
    workBW = null;
    try
    {
      workBW = new BufferedWriter(new FileWriter(wfName));
    }
    catch (IOException e)
    {
      System.out.println("   Error opening work file "+wfName+" : "+e.toString());
    }
  }

  // close work file
  private void closeWorkFile()
  {
    try
    {
      workBW.close();
    }
    catch (IOException e)
    {
      System.out.println("   Error closing work file : "+e.toString());
    }
  }

  // write line to work file
  private void writeToWorkFile( String line )
  {
    try
    {
      workBW.write(line+"\r\n");
    }
    catch (IOException e)
    {
      System.out.println("   Error writing to work file : "+e.toString());
    }
  }

  // open work file
  private void openWork2File(String wf2Name)
  {
    work2BW = null;
    try
    {
      work2BW = new BufferedWriter(new FileWriter(wf2Name));
    }
    catch (IOException e)
    {
      System.out.println("   Error opening work2 file "+wf2Name+" : "+e.toString());
    }
  }

  // close work file
  private void closeWork2File()
  {
    try
    {
      work2BW.close();
    }
    catch (IOException e)
    {
      System.out.println("   Error closing work2 file : "+e.toString());
    }
  }

  // write line to work2 file
  private void writeToWork2File( String line )
  {
    try
    {
      work2BW.write(line+"\r\n");
    }
    catch (IOException e)
    {
      System.out.println("   Error writing to work2 file : "+e.toString());
    }
  }

  // open and create extract file
  private void createExtractFile(String efName)
  {
    BufferedWriter extractBW = null;
    try
    {
      extractBW = new BufferedWriter(new FileWriter(efName));
      File wFolder = new File(workDir);
      File[] workFileArray = wFolder.listFiles();
      for(int i=0;i<workFileArray.length;i++)
      {
        BufferedReader w = new BufferedReader(new FileReader(workFileArray[i]));
        String workLine = w.readLine();
        while(workLine!=null)
        {
          extractBW.write(workLine+"\r\n");
          workLine = w.readLine();
        }
        w.close();
      }
      extractBW.close();
    }
    catch (IOException e)
    {
      System.out.println("   Error creating extract file "+efName+" : "+e.toString());
    }
  }

  // open and create extract file
  private void createExtract2File(String ef2Name)
  {
    BufferedWriter extract2BW = null;
    try
    {
      extract2BW = new BufferedWriter(new FileWriter(ef2Name));
      File wFolder = new File(workDir2);
      File[] workFileArray = wFolder.listFiles();
      for(int i=0;i<workFileArray.length;i++)
      {
        BufferedReader w = new BufferedReader(new FileReader(workFileArray[i]));
        String workLine = w.readLine();
        while(workLine!=null)
        {
          extract2BW.write(workLine+"\r\n");
          workLine = w.readLine();
        }
        w.close();
      }
      extract2BW.close();
    }
    catch (IOException e)
    {
      System.out.println("   Error creating extract2 file "+ef2Name+" : "+e.toString());
    }
  }

  private String headerLine(
    String Billing_Source,
    long File_No,
    long Record_Seq_No,
    String File_Creation_Date )
  {
    String outputLine =
      feedLine(
        Billing_Source,
        Long.toString(File_No),
        Long.toString(Record_Seq_No),
        HEADER,
        File_Creation_Date,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY );
    return outputLine;
  }

  private String trailerLine(
    String Billing_Source,
    long File_No,
    long Record_Seq_No,
    long File_Record_Count )
  {
    String outputLine =
      feedLine(
        Billing_Source,
        Long.toString(File_No),
        Long.toString(Record_Seq_No),
        TRAILER,
        EMPTY,
        Long.toString(File_Record_Count),
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY );
    return outputLine;
  }

  private String accountLine(
    String Billing_Source,
    long File_No,
    long Record_Seq_No,
    String Customer_Reference,
    String Account_Name,
    String Invoice_No,
    String Account_Effective_From,
    String GL_Name,
    String GL_Code,
    String GL_Customer_Number,
    String Outgoing_Currency_Code,
    String CW_Company_Name,
    String Billing_Contact,
    String Billing_Customer_Name,
    String CW_Contact,
    String Customer_Contact,
    String Account_Reference,
    String Tax_Reference_Literal,
    String Tax_Reference,
    String Jurisdiction_Currency_Code )
  {
    String outputLine =
      feedLine(
        Billing_Source,
        Long.toString(File_No),
        Long.toString(Record_Seq_No),
        ACCOUNT,
        EMPTY,
        EMPTY,
        Customer_Reference,
        Account_Name,
        Invoice_No,
        Account_Effective_From,
        EMPTY,
        GL_Name,
        GL_Code,
        GL_Customer_Number,
        Outgoing_Currency_Code,
        CW_Company_Name,
        SINGLEN,
        Billing_Contact,
        Billing_Customer_Name,
        CW_Contact,
        Customer_Contact,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        Account_Reference,
        Tax_Reference_Literal,
        Tax_Reference,
        Jurisdiction_Currency_Code );
    return outputLine;
  }

  private String addressLine(
    String Billing_Source,
    long File_No,
    long Record_Seq_No,
    String Customer_Reference,
    String Account_Name,
    String Invoice_No,
    String Address_Type,
    String Address_Effective_From,
    String Address_Line_1,
    String Address_Line_2,
    String Address_Line_3,
    String Address_Line_4,
    String Address_Line_5,
    String Address_Line_6,
    String Address_Line_7,
    String Address_Line_8,
    String Address_Line_9,
    String Address_Line_10 )
  {
    String outputLine =
      feedLine(
        Billing_Source,
        Long.toString(File_No),
        Long.toString(Record_Seq_No),
        ADDRESS,
        EMPTY,
        EMPTY,
        Customer_Reference,
        Account_Name,
        Invoice_No,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        Address_Type,
        Address_Effective_From,
        EMPTY,
        Address_Line_1,
        Address_Line_2,
        Address_Line_3,
        Address_Line_4,
        Address_Line_5,
        Address_Line_6,
        Address_Line_7,
        Address_Line_8,
        Address_Line_9,
        Address_Line_10,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY );
    return outputLine;
  }

  private String bankLine(
    String Billing_Source,
    long File_No,
    long Record_Seq_No,
    String Customer_Reference,
    String Account_Name,
    String Invoice_No,
    String Reason_Code,
    String Bank_Effective_From,
    String Bank_Address_Line_1,
    String Bank_Address_Line_2,
    String Bank_Address_Line_3,
    String Bank_Address_Line_4,
    String Bank_Address_Line_5,
    String Bank_Address_Line_6,
    String Bank_Address_Line_7,
    String Bank_Address_Line_8,
    String Bank_Address_Line_9,
    String Bank_Address_Line_10 )
  {
    String outputLine =
      feedLine(
        Billing_Source,
        Long.toString(File_No),
        Long.toString(Record_Seq_No),
        BANK,
        EMPTY,
        EMPTY,
        Customer_Reference,
        Account_Name,
        Invoice_No,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        Reason_Code,
        Bank_Effective_From,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        Bank_Address_Line_1,
        Bank_Address_Line_2,
        Bank_Address_Line_3,
        Bank_Address_Line_4,
        Bank_Address_Line_5,
        Bank_Address_Line_6,
        Bank_Address_Line_7,
        Bank_Address_Line_8,
        Bank_Address_Line_9,
        Bank_Address_Line_10,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY );
    return outputLine;
  }

  private String invoiceTotalLine(
    String Billing_Source,
    long File_No,
    long Record_Seq_No,
    String Customer_Reference,
    String Account_Name,
    String Invoice_No,
    String Issue_Date,
    String Tax_Point_Date,
    String Period_From,
    String Period_To,
    String Invoice_Total,
    String Invoice_Type )
  {
    String outputLine =
      feedLine(
        Billing_Source,
        Long.toString(File_No),
        Long.toString(Record_Seq_No),
        INVOICE,
        EMPTY,
        EMPTY,
        Customer_Reference,
        Account_Name,
        Invoice_No,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        SINGLEI,
        SINGLEL,
        ZERO,
        Issue_Date,
        Tax_Point_Date,
        Period_From,
        Period_To,
        Invoice_Total,
        Invoice_Type,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY );
    return outputLine;
  }

  private String invoiceLine(
    String Billing_Source,
    long File_No,
    long Record_Seq_No,
    String Customer_Reference,
    String Account_Name,
    String Invoice_No,
    String Line_Type,
    String Component_Date,
    String Amount,
    String Local_Amount,
    String Local_Currency,
    String Billing_Currency,
    String Description,
    String Exchange_Rate,
    String Rate,
    String Product_Name )
  {
    String outputLine =
      feedLine(
        Billing_Source,
        Long.toString(File_No),
        Long.toString(Record_Seq_No),
        INVOICE,
        EMPTY,
        EMPTY,
        Customer_Reference,
        Account_Name,
        Invoice_No,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        SINGLEI,
        SINGLEL,
        ONE,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        Line_Type,
        Component_Date,
        Amount,
        Local_Amount,
        Local_Currency,
        Billing_Currency,
        Description,
        Exchange_Rate,
        Rate,
        Product_Name,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY );
    return outputLine;
  }

  private String invoiceDetail(
    String Billing_Source,
    long File_No,
    long Record_Seq_No,
    String Customer_Reference,
    String Account_Name,
    String Invoice_No,
    String Line_Type,
    String Component_Date,
    String Amount,
    String Local_Amount,
    String Local_Currency,
    String Billing_Currency,
    String Description,
    String Exchange_Rate,
    String Rate,
    String Service_Name )
  {
    String outputLine =
      feedLine(
        Billing_Source,
        Long.toString(File_No),
        Long.toString(Record_Seq_No),
        INVOICE,
        EMPTY,
        EMPTY,
        Customer_Reference,
        Account_Name,
        Invoice_No,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        SINGLEI,
        SINGLED,
        ZERO,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        Line_Type,
        Component_Date,
        Amount,
        Local_Amount,
        Local_Currency,
        Billing_Currency,
        Description,
        Exchange_Rate,
        Rate,
        EMPTY,
        Service_Name,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY );
    return outputLine;
  }

  private String sourceData(
    String Billing_Source,
    long File_No,
    long Record_Seq_No,
    String Customer_Reference,
    String Account_Name,
    String Invoice_No,
    String Source_Data_Name,
    String Format,
    String Length,
    String Decimal_Places,
    String String_Value,
    String Number_Value,
    String Date_Value )
  {
    String outputLine =
      feedLine(
        Billing_Source,
        Long.toString(File_No),
        Long.toString(Record_Seq_No),
        SOURCE,
        EMPTY,
        EMPTY,
        Customer_Reference,
        Account_Name,
        Invoice_No,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        Source_Data_Name,
        Format,
        Length,
        Decimal_Places,
        String_Value,
        Number_Value,
        Date_Value,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY );
    return outputLine;
  }

  private String supplementaryLine(
    String Billing_Source,
    long File_No,
    long Record_Seq_No,
    String Customer_Reference,
    String Account_Name,
    String Invoice_No,
    String Line_Type,
    String Component_Date,
    String Amount,
    String Local_Amount,
    String Local_Currency,
    String Billing_Currency,
    String Description,
    String Exchange_Rate,
    String Rate,
    String Product_Name )
  {
    String outputLine =
      feedLine(
        Billing_Source,
        Long.toString(File_No),
        Long.toString(Record_Seq_No),
        INVOICE,
        EMPTY,
        EMPTY,
        Customer_Reference,
        Account_Name,
        Invoice_No,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        SINGLES,
        SINGLEL,
        ONE,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        Line_Type,
        Component_Date,
        Amount,
        Local_Amount,
        Local_Currency,
        Billing_Currency,
        Description,
        Exchange_Rate,
        Rate,
        Product_Name,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY );
    return outputLine;
  }

  private String supplementaryDetail(
    String Billing_Source,
    long File_No,
    long Record_Seq_No,
    String Customer_Reference,
    String Account_Name,
    String Invoice_No,
    String Line_Type,
    String Component_Date,
    String Amount,
    String Local_Amount,
    String Local_Currency,
    String Billing_Currency,
    String Description,
    String Exchange_Rate,
    String Rate,
    String Service_Name )
  {
    String outputLine =
      feedLine(
        Billing_Source,
        Long.toString(File_No),
        Long.toString(Record_Seq_No),
        INVOICE,
        EMPTY,
        EMPTY,
        Customer_Reference,
        Account_Name,
        Invoice_No,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        SINGLES,
        SINGLED,
        ZERO,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        Line_Type,
        Component_Date,
        Amount,
        Local_Amount,
        Local_Currency,
        Billing_Currency,
        Description,
        Exchange_Rate,
        Rate,
        EMPTY,
        Service_Name,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY,
        EMPTY );
    return outputLine;
  }

  private String feedLine(
    String Billing_Source,
    String File_No,
    String Record_Seq_No,
    String Record_Type,
    String File_Creation_Date,
    String File_Record_Count,
    String Customer_Reference,
    String Account_Name,
    String Invoice_No,
    String Account_Effective_From,
    String Account_Effective_To,
    String GL_Name,
    String GL_Code,
    String GL_Customer_Number,
    String Outgoing_Currency_Code,
    String CW_Company_Name,
    String Credit_Immediate,
    String Billing_Contact,
    String Billing_Customer_Name,
    String CW_Contact,
    String Customer_Contact,
    String Reason_Code,
    String Bank_Effective_From,
    String Bank_Effective_To,
    String Bank_Account_Number,
    String Bank_Sort_Code,
    String Bank_Name,
    String Bank_Address_Line_1,
    String Bank_Address_Line_2,
    String Bank_Address_Line_3,
    String Bank_Address_Line_4,
    String Bank_Address_Line_5,
    String Bank_Address_Line_6,
    String Bank_Address_Line_7,
    String Bank_Address_Line_8,
    String Bank_Address_Line_9,
    String Bank_Address_Line_10,
    String Address_Type,
    String Address_Effective_From,
    String Address_Effective_To,
    String Address_Line_1,
    String Address_Line_2,
    String Address_Line_3,
    String Address_Line_4,
    String Address_Line_5,
    String Address_Line_6,
    String Address_Line_7,
    String Address_Line_8,
    String Address_Line_9,
    String Address_Line_10,
    String Invoice_Supplementary_Ind,
    String Line_Detail_Ind,
    String Level_Ind,
    String Issue_Date,
    String Tax_Point_Date,
    String Period_From,
    String Period_To,
    String Invoice_Total,
    String Invoice_Type,
    String Line_Type,
    String Component_Date,
    String Amount,
    String Local_Amount,
    String Local_Currency,
    String Billing_Currency,
    String Description,
    String Exchange_Rate,
    String Rate,
    String Product_Name,
    String Service_Name,
    String Source_Data_Name,
    String Format,
    String Length,
    String Decimal_Places,
    String String_Value,
    String Number_Value,
    String Date_Value,
    String Account_Reference,
    String Tax_Reference_Literal,
    String Tax_Reference,
    String Jurisdiction_Currency_Code )
  {
    String outputLine =
      Billing_Source+VPIPE+
      File_No+VPIPE+
      Record_Seq_No+VPIPE+
      Record_Type+VPIPE+
      File_Creation_Date+VPIPE+
      File_Record_Count+VPIPE+
      Customer_Reference+VPIPE+
      Account_Name+VPIPE+
      Invoice_No+VPIPE+
      Account_Effective_From+VPIPE+
      Account_Effective_To+VPIPE+
      GL_Name+VPIPE+
      GL_Code+VPIPE+
      GL_Customer_Number+VPIPE+
      Outgoing_Currency_Code+VPIPE+
      CW_Company_Name+VPIPE+
      Credit_Immediate+VPIPE+
      Billing_Contact+VPIPE+
      Billing_Customer_Name+VPIPE+
      CW_Contact+VPIPE+
      Customer_Contact+VPIPE+
      Reason_Code+VPIPE+
      Bank_Effective_From+VPIPE+
      Bank_Effective_To+VPIPE+
      Bank_Account_Number+VPIPE+
      Bank_Sort_Code+VPIPE+
      Bank_Name+VPIPE+
      Bank_Address_Line_1+VPIPE+
      Bank_Address_Line_2+VPIPE+
      Bank_Address_Line_3+VPIPE+
      Bank_Address_Line_4+VPIPE+
      Bank_Address_Line_5+VPIPE+
      Bank_Address_Line_6+VPIPE+
      Bank_Address_Line_7+VPIPE+
      Bank_Address_Line_8+VPIPE+
      Bank_Address_Line_9+VPIPE+
      Bank_Address_Line_10+VPIPE+
      Address_Type+VPIPE+
      Address_Effective_From+VPIPE+
      Address_Effective_To+VPIPE+
      Address_Line_1+VPIPE+
      Address_Line_2+VPIPE+
      Address_Line_3+VPIPE+
      Address_Line_4+VPIPE+
      Address_Line_5+VPIPE+
      Address_Line_6+VPIPE+
      Address_Line_7+VPIPE+
      Address_Line_8+VPIPE+
      Address_Line_9+VPIPE+
      Address_Line_10+VPIPE+
      Invoice_Supplementary_Ind+VPIPE+
      Line_Detail_Ind+VPIPE+
      Level_Ind+VPIPE+
      Issue_Date+VPIPE+
      Tax_Point_Date+VPIPE+
      Period_From+VPIPE+
      Period_To+VPIPE+
      Invoice_Total+VPIPE+
      Invoice_Type+VPIPE+
      Line_Type+VPIPE+
      Component_Date+VPIPE+
      Amount+VPIPE+
      Local_Amount+VPIPE+
      Local_Currency+VPIPE+
      Billing_Currency+VPIPE+
      Description+VPIPE+
      Exchange_Rate+VPIPE+
      Rate+VPIPE+
      Product_Name+VPIPE+
      Service_Name+VPIPE+
      Source_Data_Name+VPIPE+
      Format+VPIPE+
      Length+VPIPE+
      Decimal_Places+VPIPE+
      String_Value+VPIPE+
      Number_Value+VPIPE+
      Date_Value+VPIPE+
      Account_Reference+VPIPE+
      Tax_Reference_Literal+VPIPE+
      Tax_Reference+VPIPE+
      Jurisdiction_Currency_Code;
    return outputLine;
  }

  // handle numbers returned from SQL Server returned as strings
  // insert missing zeroes before decimal place and rebuild
  // exponential numbers as staright numbers
  private String tidyNumericString (String input)
  {
    //System.out.println(input);
        // check for null
    String result = "";
    String interim = input;
    try
    {
      // check for exponential number
      String baseNumber = "";
      String power = "";
      boolean foundE = false;
      for (int i=0; i<interim.length(); i++)
      {
        String testChar = interim.substring(i,i+1);
        if (foundE)
          power = power+testChar;
        else
        {
          if (testChar.startsWith(SINGLEE))
            foundE = true;
          else
            baseNumber = baseNumber+testChar;
        }
      }
      // if exponential number then resconstruct number
      if (foundE)
      {
        float startNumber = Float.parseFloat(baseNumber);
        float finalNumber = 0;
        float multiplier = 0;
        String mult = "";
        int counter = Integer.parseInt(power.substring(1,power.trim().length()));
        if (power.startsWith(HYPHEN))
        {
          mult = ZERO+PERIOD;
          for (int i=1; i<counter; i++)
          {
            mult = mult+ZERO;
          }
          mult = mult+ONE;
        }
        else
        {
          mult = ONE;
          for (int i=1; i==counter ; i++)
          {
            mult = mult+ZERO;
          }
        }
        multiplier = Float.parseFloat(mult);
        finalNumber = Float.parseFloat(baseNumber) * multiplier;
        interim = Float.toString(finalNumber);
      }
      // add additional zero at start if number starts with decimal place
      if (interim.startsWith(PERIOD))
        interim=ZERO+interim;
      result = interim;
    }
    catch(java.lang.Exception ex) // ignore exceptions
    {
      result = "";
    }
    //System.out.println(result);
    return result;
  }

  // remove carriage returns, line feeds and tabs from strings and reset NULL
  // value to empty string
  private String tidyString(String input)
  {
    String result = "";
    if (input==null)
      result = "";
    else if (input=="")
      result = "";
    else if (input.startsWith(NULL))
      result = "";
    else
    {
      result = input.replace('\r',' ');
      result = result.replace('\n',' ');
      result = result.replace('\t',' ');
      result = result.trim();
    }
    return result;
  }

  // Just reset NULL to empty string
  private String tidyString2(String input)
  {
    String result = "";
    if (input==null)
      result = "";
    else if (input.startsWith("null"))
      result = "";
    else
      result = input;
    return result;
  }

  private boolean conglomSwitchedBilledNoSummaryNoLD(String globalCustomerId, String accountName, String invoiceNo)
  {
    boolean result = true;
    String rName = "cb_ssvo_billed_no_summary_nold";
    // supplementary line
    String workLine =
      supplementaryLine(
        bsName,
        workFileNo,
        recordSeqNoReports,
        globalCustomerId,
        accountName,
        invoiceNo,
        CHARGE,
        billDate,
        "0.00",
        "0.00",
        invoiceCurrency,
        invoiceCurrency,
        rName,
        "1.00",
        "",
        CONGLOM);
    writeToWork2File(workLine);
    recordSeqNoReports++;
    ResultSet reportRS = eDB.getReportDetails(invoiceNo,rName);
    try
    {
      while (reportRS.next())
      {
        String subTotalLevel1 = tidyString(reportRS.getString("Sub_Total_Level1"));
        String subTotalLevel2 = tidyString(reportRS.getString("Sub_Total_Level2"));
        String subTField1 = tidyString(reportRS.getString("SubT_Field_1"));
        String subTField2 = tidyString(reportRS.getString("SubT_Field_2"));
        String conglomCustName = tidyString(reportRS.getString("Conglom_Cust_Name"));
        String cwContactTitle = tidyString(reportRS.getString("CW_Contact_Title"));
        String cwContactName = tidyString(reportRS.getString("CW_Contact_Name"));
        String conglomInvoiceRef = tidyString(reportRS.getString("Conglom_Invoice_Ref"));
        String billDate = reformatConglomDate(reportRS.getString("Bill_Date"));
        String invoiceCurrency = tidyString(reportRS.getString("Invoice_Currency"));
        String billPeriodRef = tidyString(reportRS.getString("Bill_Period_Ref"));
        String sourceAccountNo = tidyString(reportRS.getString("Source_Account_No"));
        String billingNumber = tidyString(reportRS.getString("Billing_Number"));
        String billNumberExpanded = tidyString(reportRS.getString("Bill_Number_Expanded"));
        String gvpnInd = tidyString(reportRS.getString("Gvpn_Ind"));
        String costCentre = tidyString(reportRS.getString("Cost_Centre"));
        String durationMinutes = tidyString(reportRS.getString("Duration_Minutes"));
        String durationSeconds = tidyString(reportRS.getString("Duration_Seconds"));
        String numberOfCalls = tidyString(reportRS.getString("Number_Of_Calls"));
        String billingChargePSTN = tidyString(reportRS.getString("Billing_Charge_PSTN"));
        String billingChargeVPN = tidyString(reportRS.getString("Billing_Charge_VPN"));
        String billNumberCharge = tidyString(reportRS.getString("Bill_Number_Charge"));
        String billNumberDiscount = tidyString(reportRS.getString("Bill_Number_Discount"));
        String billNumberTotal = tidyString(reportRS.getString("Bill_Number_Total"));
        // supplementary detail
        workLine =
          supplementaryDetail(
            bsName,
            workFileNo,
            recordSeqNoReports,
            globalCustomerId,
            accountName,
            invoiceNo,
            CHARGE,
            billDate,
            "0.00",
            "0.00",
            invoiceCurrency,
            invoiceCurrency,
            rName,
            "1.00",
            "",
            CONGLOM);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        // source data
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Sub_Total_Level1",STRING,"3","",subTotalLevel1,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Sub_Total_Level2",STRING,"3","",subTotalLevel2,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "SubT_Field_1",STRING,"3","",subTField1,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "SubT_Field_2",STRING,"3","",subTField2,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Conglom_Cust_Name",STRING,"50","",conglomCustName,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "CW_Contact_Title",STRING,"31","",cwContactTitle,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "CW_Contact_Name",STRING,"30","",cwContactName,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Conglom_Invoice_Ref",STRING,"13","",conglomInvoiceRef,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Bill_Date",DATE,"","","","",billDate);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Invoice_Currency",STRING,"3","",invoiceCurrency,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Bill_Period_Ref",STRING,"4","",billPeriodRef,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Source_Account_No",STRING,"10","",sourceAccountNo,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Billing_Number",STRING,"13","",billingNumber,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Bill_Number_Expanded",STRING,"50","",billNumberExpanded,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Gvpn_Ind",STRING,"1","",gvpnInd,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Cost_Centre",STRING,"3","",costCentre,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Duration_Minutes",NUMBER,"8","0","",durationMinutes,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Duration_Seconds",NUMBER,"2","0","",durationSeconds,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Number_Of_Calls",NUMBER,"8","0","",numberOfCalls,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Billing_Charge_PSTN",NUMBER,"10","4","",billingChargePSTN,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Billing_Charge_VPN",NUMBER,"10","4","",billingChargeVPN,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Bill_Number_Charge",NUMBER,"10","4","",billNumberCharge,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Bill_Number_Discount",NUMBER,"12","4","",billNumberDiscount,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Bill_Number_Total",NUMBER,"12","4","",billNumberTotal,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
      }
    }
    catch(java.sql.SQLException ex)
    {
      message = "   Error getting switched billed no summary no local data report details for conglom invoice "+invoiceNo+" : "+ex.toString();
      System.out.println(message);
      writeToLogFile(message);
      result = false;
    }
    finally
    {
      reportRS=null;
    }
    return result;
  }

  private boolean conglomSwitchedBilledNoSummaryLD(String globalCustomerId, String accountName, String invoiceNo)
  {
    boolean result = true;
    String rName = "cb_ssvo_billed_no_summary";
    // supplementary line
    String workLine =
      supplementaryLine(
        bsName,
        workFileNo,
        recordSeqNoReports,
        globalCustomerId,
        accountName,
        invoiceNo,
        CHARGE,
        billDate,
        "0.00",
        "0.00",
        invoiceCurrency,
        invoiceCurrency,
        rName,
        "1.00",
        "",
        CONGLOM);
    writeToWork2File(workLine);
    recordSeqNoReports++;
    if (eDB.getLDCount(invoiceNo,BILSUM,SSVO)>0)
    {
      ResultSet reportRS = eDB.getReportDetails(invoiceNo,rName);
      try
      {
        while (reportRS.next())
        {
          String subTotalLevel1 = tidyString(reportRS.getString("Sub_Total_Level1"));
          String subTotalLevel2 = tidyString(reportRS.getString("Sub_Total_Level2"));
          String subTField1 = tidyString(reportRS.getString("SubT_Field_1"));
          String subTField2 = tidyString(reportRS.getString("SubT_Field_2"));
          String conglomCustName = tidyString(reportRS.getString("Conglom_Cust_Name"));
          String cwContactTitle = tidyString(reportRS.getString("CW_Contact_Title"));
          String cwContactName = tidyString(reportRS.getString("CW_Contact_Name"));
          String conglomInvoiceRef = tidyString(reportRS.getString("Conglom_Invoice_Ref"));
          String billDate = reformatConglomDate(reportRS.getString("Bill_Date"));
          String invoiceCurrency = tidyString(reportRS.getString("Invoice_Currency"));
          String billPeriodRef = tidyString(reportRS.getString("Bill_Period_Ref"));
          String sourceAccountNo = tidyString(reportRS.getString("Source_Account_No"));
          String billingNumber = tidyString(reportRS.getString("Billing_Number"));
          String billNumberExpanded = tidyString(reportRS.getString("Bill_Number_Expanded"));
          String gvpnInd = tidyString(reportRS.getString("Gvpn_Ind"));
          String costCentre = tidyString(reportRS.getString("Cost_Centre"));
          String durationMinutes = tidyString(reportRS.getString("Duration_Minutes"));
          String durationSeconds = tidyString(reportRS.getString("Duration_Seconds"));
          String numberOfCalls = tidyString(reportRS.getString("Number_Of_Calls"));
          String billingChargePSTN = tidyString(reportRS.getString("Billing_Charge_PSTN"));
          String billingChargeVPN = tidyString(reportRS.getString("Billing_Charge_VPN"));
          String billNumberCharge = tidyString(reportRS.getString("Bill_Number_Charge"));
          String billNumberDiscount = tidyString(reportRS.getString("Bill_Number_Discount"));
          String billNumberTotal = tidyString(reportRS.getString("Bill_Number_Total"));
          String ldSortItem1 = tidyString(reportRS.getString("LD_Sort_Item1"));
          String ldSortItem2 = tidyString(reportRS.getString("LD_Sort_Item2"));
          String ldSortItem3 = tidyString(reportRS.getString("LD_Sort_Item3"));
          String ldHeader1 = reportRS.getString("LD_Header1");
          String ldHeader2 = reportRS.getString("LD_Header2");
          String ldHeader3 = reportRS.getString("LD_Header3");
          String reportLDString = tidyString2(reportRS.getString("Report_LD_String"));
          // supplementary detail
          workLine =
            supplementaryDetail(
              bsName,
              workFileNo,
              recordSeqNoReports,
              globalCustomerId,
              accountName,
              invoiceNo,
              CHARGE,
              billDate,
              "0.00",
              "0.00",
              invoiceCurrency,
              invoiceCurrency,
              rName,
              "1.00",
              "",
              CONGLOM);
          writeToWork2File(workLine);
          recordSeqNoReports++;
          // source data
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Sub_Total_Level1",STRING,"3","",subTotalLevel1,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Sub_Total_Level2",STRING,"3","",subTotalLevel2,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "SubT_Field_1",STRING,"3","",subTField1,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "SubT_Field_2",STRING,"3","",subTField2,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Conglom_Cust_Name",STRING,"50","",conglomCustName,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "CW_Contact_Title",STRING,"31","",cwContactTitle,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "CW_Contact_Name",STRING,"30","",cwContactName,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Conglom_Invoice_Ref",STRING,"13","",conglomInvoiceRef,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Bill_Date",DATE,"","","","",billDate);
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Invoice_Currency",STRING,"3","",invoiceCurrency,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Bill_Period_Ref",STRING,"4","",billPeriodRef,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Source_Account_No",STRING,"10","",sourceAccountNo,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Billing_Number",STRING,"13","",billingNumber,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Bill_Number_Expanded",STRING,"50","",billNumberExpanded,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Gvpn_Ind",STRING,"1","",gvpnInd,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Cost_Centre",STRING,"3","",costCentre,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Duration_Minutes",NUMBER,"8","0","",durationMinutes,"");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Duration_Seconds",NUMBER,"2","0","",durationSeconds,"");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Number_Of_Calls",NUMBER,"8","0","",numberOfCalls,"");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Billing_Charge_PSTN",NUMBER,"10","4","",billingChargePSTN,"");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Billing_Charge_VPN",NUMBER,"10","4","",billingChargeVPN,"");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Bill_Number_Charge",NUMBER,"10","4","",billNumberCharge,"");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Bill_Number_Discount",NUMBER,"12","4","",billNumberDiscount,"");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Bill_Number_Total",NUMBER,"12","4","",billNumberTotal,"");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "LD_Sort_Item1",STRING,"50","",ldSortItem1,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "LD_Sort_Item2",STRING,"50","",ldSortItem2,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "LD_Sort_Item3",STRING,"50","",ldSortItem3,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "LD_Header1",STRING,"70","",ldHeader1,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "LD_Header2",STRING,"70","",ldHeader2,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "LD_Header3",STRING,"70","",ldHeader3,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Report_LD_String",STRING,"50","",reportLDString,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
        }
      }
      catch(java.sql.SQLException ex)
      {
        message = "   Error getting switched billed no summary no local data report details for conglom invoice "+invoiceNo+" : "+ex.toString();
        System.out.println(message);
        writeToLogFile(message);
        result = false;
      }
      finally
      {
        reportRS=null;
      }
    }
    return result;
  }

  private boolean conglomEbillBilledNoSummary(String globalCustomerId, String accountName, String invoiceNo)
  {
    boolean result = true;
    String rName = "ebill_billed_no_summary";
    // supplementary line
    String workLine =
      supplementaryLine(
        bsName,
        workFileNo,
        recordSeqNoReports,
        globalCustomerId,
        accountName,
        invoiceNo,
        CHARGE,
        billDate,
        "0.00",
        "0.00",
        invoiceCurrency,
        invoiceCurrency,
        rName,
        "1.00",
        "",
        CONGLOM);
    writeToWork2File(workLine);
    recordSeqNoReports++;
    ResultSet reportRS = eDB.getReportDetails(invoiceNo,rName);
    try
    {
          while (reportRS.next())
      {
        String conglomCustName = tidyString(reportRS.getString("Conglom_Cust_Name"));
        String conglomInvoiceRef = tidyString(reportRS.getString("Conglom_Invoice_Ref"));
        String billDate = reformatConglomDate(reportRS.getString("Bill_Date"));
        String invoiceCurrency = tidyString(reportRS.getString("Invoice_Currency"));
        String billPeriodRef = tidyString(reportRS.getString("Bill_Period_Ref"));
        String sourceAccountNo = tidyString(reportRS.getString("Source_Account_No"));
        String billingNumber = tidyString(reportRS.getString("Billing_Number"));
        String ldDataItem1 = tidyString(reportRS.getString("LD_Data_Item1"));
        String ldDataItem2 = tidyString(reportRS.getString("LD_Data_Item2"));
        String ldDataItem3 = tidyString(reportRS.getString("LD_Data_Item3"));
        String ldDataItem4 = tidyString(reportRS.getString("LD_Data_Item4"));
        String ldDataItem5 = tidyString(reportRS.getString("LD_Data_Item5"));
        String durationMinutes = tidyString(reportRS.getString("Duration_Minutes"));
        String durationSeconds = tidyString(reportRS.getString("Duration_Seconds"));
        String numberOfCalls = tidyString(reportRS.getString("Number_Of_Calls"));
        String billingChargePSTN = tidyString(reportRS.getString("Billing_Charge_PSTN"));
        String billingChargeVPN = tidyString(reportRS.getString("Billing_Charge_VPN"));
        String billingNumberCharge = tidyString(reportRS.getString("Bill_Number_Charge"));
        String billingNumberDiscount = tidyString(reportRS.getString("Bill_Number_Discount"));
        String billingNumberTotal = tidyString(reportRS.getString("Bill_Number_Total"));
        // supplementary detail
        workLine =
          supplementaryDetail(
            bsName,
            workFileNo,
            recordSeqNoReports,
            globalCustomerId,
            accountName,
            invoiceNo,
            CHARGE,
            billDate,
            "0.00",
            "0.00",
            invoiceCurrency,
            invoiceCurrency,
            rName,
            "1.00",
            "",
            CONGLOM);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        // source data
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Conglom_Cust_Name",STRING,"50","",conglomCustName,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Conglom_Invoice_Ref",STRING,"13","",conglomInvoiceRef,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Bill_Date",DATE,"","","","",billDate);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Invoice_Currency",STRING,"3","",invoiceCurrency,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Bill_Period_Ref",STRING,"4","",billPeriodRef,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Source_Account_No",STRING,"10","",sourceAccountNo,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Billing_Number",STRING,"13","",billingNumber,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "LD_Data_Item_1",STRING,"50","",ldDataItem1,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "LD_Data_Item_2",STRING,"50","",ldDataItem2,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "LD_Data_Item_3",STRING,"50","",ldDataItem3,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "LD_Data_Item_4",STRING,"50","",ldDataItem4,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "LD_Data_Item_5",STRING,"50","",ldDataItem5,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Duration_Minutes",NUMBER,"38","0","",durationMinutes,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Duration_Seconds",NUMBER,"38","0","",durationSeconds,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Number_Of_Calls",NUMBER,"38","0","",numberOfCalls,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Billing_Charge_PSTN",NUMBER,"20","5","",billingChargePSTN,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Billing_Charge_VPN",NUMBER,"20","5","",billingChargeVPN,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Billing_Number_Charge",NUMBER,"20","5","",billingNumberCharge,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Billing_Number_Discount",NUMBER,"20","5","",billingNumberDiscount,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Billing_Number_Total",NUMBER,"20","5","",billingNumberTotal,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
      }
    }
    catch(java.sql.SQLException ex)
    {
      message = "   Error getting ebill billed no summary details for conglom invoice "+invoiceNo+" : "+ex.toString();
      System.out.println(message);
      writeToLogFile(message);
      result = false;
    }
    finally
    {
      reportRS=null;
    }
    return result;
  }

  private boolean conglomTalismanOutbound(String globalCustomerId, String accountName, String invoiceNo)
  {
    boolean result = true;
    String rName = "cb_talisman_outbound";
    // supplementary line
    String workLine =
      supplementaryLine(
        bsName,
        workFileNo,
        recordSeqNoReports,
        globalCustomerId,
        accountName,
        invoiceNo,
        CHARGE,
        billDate,
        "0.00",
        "0.00",
        invoiceCurrency,
        invoiceCurrency,
        rName,
        "1.00",
        "",
        CONGLOM);
    writeToWork2File(workLine);
    recordSeqNoReports++;
    ResultSet reportRS = eDB.getReportDetails(invoiceNo,rName);
    try
    {
          while (reportRS.next())
      {
        String conglomInvoiceRef = tidyString(reportRS.getString("Conglom_Invoice_Ref"));
        String circuitNo = tidyString(reportRS.getString("Circuit_No"));
        String supplier = tidyString(reportRS.getString("Supplier"));
        String batchId = tidyString(reportRS.getString("Batch_Id"));
        String invDate = tidyString(reportRS.getString("Inv_Date"));
        String rental = tidyString(reportRS.getString("Rental"));
        String rentalFromDate = tidyString(reportRS.getString("Rental_From_Date"));
        String rentalToDate = tidyString(reportRS.getString("Rental_To_Date"));
        String usage = tidyString(reportRS.getString("Usage"));
        String usageFromDate = tidyString(reportRS.getString("Usage_From_Date"));
        String usageToDate = tidyString(reportRS.getString("Usage_To_Date"));
        String maint = tidyString(reportRS.getString("Maint"));
        String other = tidyString(reportRS.getString("Other"));
        String subTotal = tidyString(reportRS.getString("Sub_Total"));
        String VAT = tidyString(reportRS.getString("VAT"));
        String total = tidyString(reportRS.getString("total"));
        // supplementary detail
        workLine =
          supplementaryDetail(
            bsName,
            workFileNo,
            recordSeqNoReports,
            globalCustomerId,
            accountName,
            invoiceNo,
            CHARGE,
            billDate,
            "0.00",
            "0.00",
            invoiceCurrency,
            invoiceCurrency,
            rName,
            "1.00",
            "",
            CONGLOM);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Conglom_Invoice_Ref",STRING,"13","",conglomInvoiceRef,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Circuit_No",STRING,"20","",circuitNo,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Supplier",STRING,"3","",supplier,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Batch_Id",STRING,"6","",batchId,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Inv_Date",STRING,"20","",invDate,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Rental",NUMBER,"20","5","",rental,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Rental_From_Date",STRING,"20","",rentalFromDate,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Rental_To_Date",STRING,"20","",rentalToDate,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Usage",NUMBER,"20","5","",usage,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Usage_From_Date",STRING,"20","",usageFromDate,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Usage_To_Date",STRING,"20","",usageToDate,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Maint",NUMBER,"20","5","",maint,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Other",NUMBER,"20","5","",other,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Sub_Total",NUMBER,"20","5","",subTotal,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "VAT",NUMBER,"20","5","",VAT,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Total",NUMBER,"20","5","",total,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
      }
    }
    catch(java.sql.SQLException ex)
    {
      message = "   Error getting talisman outbound report details for conglom invoice "+invoiceNo+" : "+ex.toString();
      System.out.println(message);
      writeToLogFile(message);
      result = false;
    }
    finally
    {
      reportRS=null;
    }
    return result;
  }

  private boolean conglomTalismanInbound(String globalCustomerId, String accountName, String invoiceNo)
  {
    boolean result = true;
    String rName = "cb_talisman_inbound";
    // supplementary line
    String workLine =
      supplementaryLine(
        bsName,
        workFileNo,
        recordSeqNoReports,
        globalCustomerId,
        accountName,
        invoiceNo,
        CHARGE,
        billDate,
        "0.00",
        "0.00",
        invoiceCurrency,
        invoiceCurrency,
        rName,
        "1.00",
        "",
        CONGLOM);
    writeToWork2File(workLine);
    recordSeqNoReports++;
    ResultSet reportRS = eDB.getReportDetails(invoiceNo,rName);
    try
    {
          while (reportRS.next())
      {
        String conglomInvoiceRef = tidyString(reportRS.getString("Conglom_Invoice_Ref"));
        String circuitNo = tidyString(reportRS.getString("Circuit_No"));
        String supplier = tidyString(reportRS.getString("Supplier"));
        String batchId = tidyString(reportRS.getString("Batch_Id"));
        String invDate = tidyString(reportRS.getString("Inv_Date"));
        String rental = tidyString(reportRS.getString("Rental"));
        String rentalFromDate = tidyString(reportRS.getString("Rental_From_Date"));
        String rentalToDate = tidyString(reportRS.getString("Rental_To_Date"));
        String usage = tidyString(reportRS.getString("Usage"));
        String usageFromDate = tidyString(reportRS.getString("Usage_From_Date"));
        String usageToDate = tidyString(reportRS.getString("Usage_To_Date"));
        String maint = tidyString(reportRS.getString("Maint"));
        String other = tidyString(reportRS.getString("Other"));
        String subTotal = tidyString(reportRS.getString("Sub_Total"));
        String VAT = tidyString(reportRS.getString("VAT"));
        String total = tidyString(reportRS.getString("total"));
        // supplementary detail
        workLine =
          supplementaryDetail(
            bsName,
            workFileNo,
            recordSeqNoReports,
            globalCustomerId,
            accountName,
            invoiceNo,
            CHARGE,
            billDate,
            "0.00",
            "0.00",
            invoiceCurrency,
            invoiceCurrency,
            rName,
            "1.00",
            "",
            CONGLOM);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Conglom_Invoice_Ref",STRING,"13","",conglomInvoiceRef,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Circuit_no",STRING,"20","",circuitNo,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Supplier",STRING,"3","",supplier,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Batch_Id",STRING,"6","",batchId,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Inv_Date",STRING,"20","",invDate,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Rental",NUMBER,"20","5","",rental,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Rental_From_Date",STRING,"20","",rentalFromDate,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Rental_To_Date",STRING,"20","",rentalToDate,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Usage",NUMBER,"20","5","",usage,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Usage_From_Date",STRING,"20","",usageFromDate,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Usage_To_Date",STRING,"20","",usageToDate,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Maint",NUMBER,"20","5","",maint,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Other",NUMBER,"20","5","",other,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Sub_Total",NUMBER,"20","5","",subTotal,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "VAT",NUMBER,"20","5","",VAT,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Total",NUMBER,"20","5","",total,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
      }
    }
    catch(java.sql.SQLException ex)
    {
      message = "   Error getting talisman inbound report details for conglom invoice "+invoiceNo+" : "+ex.toString();
      System.out.println(message);
      writeToLogFile(message);
      result = false;
    }
    finally
    {
      reportRS=null;
    }
    return result;
  }


  private boolean conglomArborTalismanData(String globalCustomerId, String accountName, String invoiceNo)
  {
    boolean result = true;
    String rName = "cb_arbor_talisman_data";
    // supplementary line
    String workLine =
      supplementaryLine(
        bsName,
        workFileNo,
        recordSeqNoReports,
        globalCustomerId,
        accountName,
        invoiceNo,
        CHARGE,
        billDate,
        "0.00",
        "0.00",
        invoiceCurrency,
        invoiceCurrency,
        rName,
        "1.00",
        "",
        CONGLOM);
    writeToWork2File(workLine);
    recordSeqNoReports++;
    ResultSet reportRS = eDB.getReportDetails(invoiceNo,rName);
    try
    {
          while (reportRS.next())
      {
        String circuitNo = tidyString(reportRS.getString("Circuit_No"));
        String supplier = tidyString(reportRS.getString("Supplier"));
        String batchId = tidyString(reportRS.getString("Batch_Id"));
        String invoiceDate = tidyString(reportRS.getString("Invoice_Date"));
        String rental = tidyString(reportRS.getString("Rental"));
        String rentalFromDate = tidyString(reportRS.getString("Rental_From_Date"));
        String rentalToDate = tidyString(reportRS.getString("Rental_To_Date"));
        String usage = tidyString(reportRS.getString("Usage"));
        String usageFromDate = tidyString(reportRS.getString("Usage_From_Date"));
        String usageToDate = tidyString(reportRS.getString("Usage_To_Date"));
        String maint = tidyString(reportRS.getString("Maint"));
        String other = tidyString(reportRS.getString("Other"));
        String subTotal = tidyString(reportRS.getString("Sub_Total"));
        String VAT = tidyString(reportRS.getString("VAT"));
        String total = tidyString(reportRS.getString("total"));
        // supplementary detail
        workLine =
          supplementaryDetail(
            bsName,
            workFileNo,
            recordSeqNoReports,
            globalCustomerId,
            accountName,
            invoiceNo,
            CHARGE,
            billDate,
            "0.00",
            "0.00",
            invoiceCurrency,
            invoiceCurrency,
            rName,
            "1.00",
            "",
            CONGLOM);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Circuit_No",STRING,"50","",circuitNo,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Supplier",STRING,"3","",supplier,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Batch_Id",STRING,"7","",batchId,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Invoice_Date",STRING,"20","",invoiceDate,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Rental",STRING,"20","",rental,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Rental_From_Date",STRING,"11","",rentalFromDate,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Rental_To_Date",STRING,"1","",rentalToDate,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Usage",STRING,"20","",usage,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Usage_From_Date",STRING,"11","",usageFromDate,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Usage_To_Date",STRING,"11","",usageToDate,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Maint",STRING,"20","",maint,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Other",STRING,"20","",other,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Sub_Total",STRING,"20","",subTotal,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "VAT",STRING,"20","",VAT,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Total",STRING,"20","",total,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
      }
    }
    catch(java.sql.SQLException ex)
    {
      message = "   Error getting arbor talisman details for conglom invoice "+invoiceNo+" : "+ex.toString();
      System.out.println(message);
      writeToLogFile(message);
      result = false;
    }
    finally
    {
      reportRS=null;
    }
    return result;
  }

  private boolean ebillConglomDownload(String globalCustomerId, String accountName, String invoiceNo)
  {
    boolean result = true;
    String rName = "ebill_conglom_download";
    // supplementary line
    String workLine =
      supplementaryLine(
        bsName,
        workFileNo,
        recordSeqNoReports,
        globalCustomerId,
        accountName,
        invoiceNo,
        CHARGE,
        billDate,
        "0.00",
        "0.00",
        invoiceCurrency,
        invoiceCurrency,
        rName,
        "1.00",
        "",
        CONGLOM);
    writeToWork2File(workLine);
    recordSeqNoReports++;
    ResultSet reportRS = eDB.getReportDetails(invoiceNo,rName);
    try
    {
          while (reportRS.next())
      {
        String conglomInvoiceRef = tidyString(reportRS.getString("Conglom_Invoice_Ref"));
        String invoicetypeCode = tidyString(reportRS.getString("Invoice_Type_Code"));
        String invoiceCurrency = tidyString(reportRS.getString("Invoice_Currency"));
        String billDate = reformatConglomDate(reportRS.getString("Bill_Date"));
        String conglomCustName = tidyString(reportRS.getString("Conglom_Cust_Name"));
        String service = tidyString(reportRS.getString("Service"));
        String billPeriodRef = tidyString(reportRS.getString("Bill_Period_Ref"));
        String sourceAccountNo = tidyString(reportRS.getString("Source_Account_No"));
        String sourceInvoiceNo = tidyString(reportRS.getString("Source_Invoice_No"));
        String billedDate = reformatConglomDate(reportRS.getString("Billed_Date"));
        String ldDataItem1 = tidyString(reportRS.getString("LD_Data_Item1"));
        String ldDataItem2 = tidyString(reportRS.getString("LD_Data_Item2"));
        String ldDataItem3 = tidyString(reportRS.getString("LD_Data_Item3"));
        String ldDataItem4 = tidyString(reportRS.getString("LD_Data_Item4"));
        String ldDataItem5 = tidyString(reportRS.getString("LD_Data_Item5"));
        String oneOffCharges = tidyString(reportRS.getString("One_Off_Charges"));
        String recurringCharges = tidyString(reportRS.getString("Recurring_Charges"));
        String callinkCharges = tidyString(reportRS.getString("Callink_Charges"));
        String vpnCharges = tidyString(reportRS.getString("VPN_Charges"));
        String callCharges = tidyString(reportRS.getString("Call_Charges"));
        String easyAccessCharges = tidyString(reportRS.getString("Easy_Access_Charges"));
        String authcodeCharges = tidyString(reportRS.getString("Authcode_Charges"));
        String usageCharges = tidyString(reportRS.getString("Usage_Charges"));
        String miscCharges = tidyString(reportRS.getString("Misc_Charges"));
        String adjustmentTotal = tidyString(reportRS.getString("Adjustment_Total"));
        String sourceDiscTotal = tidyString(reportRS.getString("Source_Disc_Total"));
        String conglomDiscTotal = tidyString(reportRS.getString("Conglom_Disc_Total"));
        String acctBalNetAmount = tidyString(reportRS.getString("Acct_Bal_Net_Amount"));
        String acctBalVatAmount = tidyString(reportRS.getString("Acct_Bal_Vat_Amount"));
        String acctBalAmount = tidyString(reportRS.getString("Acct_Bal_Amount"));
        // supplementary detail
        workLine =
          supplementaryDetail(
            bsName,
            workFileNo,
            recordSeqNoReports,
            globalCustomerId,
            accountName,
            invoiceNo,
            CHARGE,
            billDate,
            "0.00",
            "0.00",
            invoiceCurrency,
            invoiceCurrency,
            rName,
            "1.00",
            "",
            CONGLOM);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        // source data
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Conglom_Invoice_Ref",STRING,"13","",conglomInvoiceRef,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Invoice_Type_Code",STRING,"1","",invoicetypeCode,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Invoice_Currency",STRING,"3","",invoiceCurrency,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Bill_Date",DATE,"","","","",billDate);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Conglom_Cust_Name",STRING,"50","",conglomCustName,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Service",STRING,"4","",service,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Bill_Period_Ref",STRING,"4","",billPeriodRef,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Source_Account_No",STRING,"10","",sourceAccountNo,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Source_Invoice_No",STRING,"10","",sourceInvoiceNo,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Billed_Date",DATE,"","","","",billedDate);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "LD_Data_Item1",STRING,"50","",ldDataItem1,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "LD_Data_Item2",STRING,"50","",ldDataItem2,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "LD_Data_Item3",STRING,"50","",ldDataItem3,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "LD_Data_Item4",STRING,"50","",ldDataItem4,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "LD_Data_Item5",STRING,"50","",ldDataItem5,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "One_Off_Charges",NUMBER,"10","2","",oneOffCharges,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Recurring_Charges",NUMBER,"10","2","",recurringCharges,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Callink_Charges",NUMBER,"10","2","",callinkCharges,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "VPN_Charges",NUMBER,"10","2","",vpnCharges,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Call_Charges",NUMBER,"10","2","",callCharges,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Easy_Access_Charges",NUMBER,"10","2","",easyAccessCharges,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Authcode_Charges",NUMBER,"10","2","",authcodeCharges,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Usage_Charges",NUMBER,"10","2","",usageCharges,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Misc_Charges",NUMBER,"10","2","",miscCharges,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Adjustment_Total",NUMBER,"10","2","",adjustmentTotal,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Source_Disc_Total",NUMBER,"10","2","",sourceDiscTotal,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Conglom_Disc_Total",NUMBER,"10","2","",conglomDiscTotal,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Acct_Bal_Net_Amount",NUMBER,"10","2","",acctBalNetAmount,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Acct_Bal_Vat_Amount",NUMBER,"10","2","",acctBalVatAmount,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Acct_Bal_Amount",NUMBER,"10","2","",acctBalAmount,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
      }
    }
    catch(java.sql.SQLException ex)
    {
      message = "   Error getting ebill conglom download report details for conglom invoice "+invoiceNo+" : "+ex.toString();
      System.out.println(message);
      writeToLogFile(message);
      result = false;
    }
    finally
    {
      reportRS=null;
    }
    return result;
  }

  private boolean conglomSwitchedDiscountSummary(String globalCustomerId, String accountName, String invoiceNo)
  {
    boolean result = true;
    String rName = "cb_ssvo_discount_summary_subt";
    // supplementary line
    String workLine =
      supplementaryLine(
        bsName,
        workFileNo,
        recordSeqNoReports,
        globalCustomerId,
        accountName,
        invoiceNo,
        CHARGE,
        billDate,
        "0.00",
        "0.00",
        invoiceCurrency,
        invoiceCurrency,
        rName,
        "1.00",
        "",
        CONGLOM);
    writeToWork2File(workLine);
    recordSeqNoReports++;
    ResultSet reportRS = eDB.getReportDetails(invoiceNo,rName);
    if (reportRS!=null)
    {
      try
      {
        while (reportRS.next())
        {
          String subTotalLevel1 = tidyString(reportRS.getString("Sub_Total_Level1"));
          String subTotalLevel2 = tidyString(reportRS.getString("Sub_Total_Level2"));
          String subtField1 = tidyString(reportRS.getString("SubT_Field1"));
          String subtField2 = tidyString(reportRS.getString("SubT_Field2"));
          String reportLDString = tidyString2(reportRS.getString("Report_LD_String"));
          String conglomCustName = tidyString(reportRS.getString("Conglom_Cust_Name"));
          String cwContactTitle = tidyString(reportRS.getString("CW_Contact_Title"));
          String cwContactName = tidyString(reportRS.getString("CW_Contact_Name"));
          String conglomInvoiceRef = tidyString(reportRS.getString("Conglom_Invoice_Ref"));
          String billDate = reformatConglomDate(reportRS.getString("Bill_Date"));
          String invoiceCurrency = tidyString(reportRS.getString("Invoice_Currency"));
          String billPeriodRef = tidyString(reportRS.getString("Bill_Period_Ref"));
          String sourceAccountNo = tidyString(reportRS.getString("Source_Account_No"));
          String sourceInvoiceNo = tidyString(reportRS.getString("Source_Invoice_No"));
          String billedDate = reformatConglomDate(reportRS.getString("Billed_Date"));
          String ldHeader1 = reportRS.getString("LD_Header1");
          String ldHeader2 = reportRS.getString("LD_Header2");
          String ldHeader3 = reportRS.getString("LD_Header3");
          String discountItemDesc1 = tidyString(reportRS.getString("Discount_Item_Desc_1"));
          String discountItemCode1 = tidyString(reportRS.getString("Discount_Item_Code1"));
          String discountableValue1 = tidyString(reportRS.getString("Discountable_Value1"));
          String discountRate1 = tidyString(reportRS.getString("Discount_Rate1"));
          String discountAmount1 = tidyString(reportRS.getString("Discount_Amount1"));
          String discountItemDesc2 = tidyString(reportRS.getString("Discount_Item_Desc_2"));
          String discountItemCode2 = tidyString(reportRS.getString("Discount_Item_Code2"));
          String discountableValue2 = tidyString(reportRS.getString("Discountable_Value2"));
          String discountRate2 = tidyString(reportRS.getString("Discount_Rate2"));
          String discountAmount2 = tidyString(reportRS.getString("Discount_Amount2"));
          String discountItemDesc3 = tidyString(reportRS.getString("Discount_Item_Desc_3"));
          String discountItemCode3 = tidyString(reportRS.getString("Discount_Item_Code3"));
          String discountableValue3 = tidyString(reportRS.getString("Discountable_Value3"));
          String discountRate3 = tidyString(reportRS.getString("Discount_Rate3"));
          String discountAmount3 = tidyString(reportRS.getString("Discount_Amount3"));
          String totalAmount = tidyString(reportRS.getString("Total_Amount"));
          // supplementary detail
          workLine =
            supplementaryDetail(
              bsName,
              workFileNo,
              recordSeqNoReports,
              globalCustomerId,
              accountName,
              invoiceNo,
              CHARGE,
              billDate,
              "0.00",
              "0.00",
              invoiceCurrency,
              invoiceCurrency,
              rName,
              "1.00",
              "",
              CONGLOM);
          writeToWork2File(workLine);
          recordSeqNoReports++;
          // source data
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Sub_Total_Level1",STRING,"3","",subTotalLevel1,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Sub_Total_Level2",STRING,"3","",subTotalLevel2,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "SubT_Field1",STRING,"50","",subtField1,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "SubT_Field2",STRING,"50","",subtField2,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Conglom_Cust_Name",STRING,"50","",conglomCustName,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "CW_Contact_Title",STRING,"31","",cwContactTitle,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "CW_Contact_Name",STRING,"30","",cwContactName,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Conglom_Invoice_Ref",STRING,"13","",conglomInvoiceRef,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Bill_Date",DATE,"","","","",billDate);
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Invoice_Currency",STRING,"3","",invoiceCurrency,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Bill_Period_Ref",STRING,"4","",billPeriodRef,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Source_Account_No",STRING,"10","",sourceAccountNo,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Source_Invoice_No",STRING,"10","",sourceInvoiceNo,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Billed_Date",DATE,"","","","",billedDate);
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "LD_Header1",STRING,"70","",ldHeader1,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "LD_Header2",STRING,"70","",ldHeader2,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "LD_Header3",STRING,"70","",ldHeader3,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Report_LD_String",STRING,"50","",reportLDString,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Discount_Item_Desc_1",STRING,"25","",discountItemDesc1,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Discount_Item_Code1",STRING,"4","",discountItemCode1,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Discountable_Value1",NUMBER,"10","2","",discountableValue1,"");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Discount_Rate1",NUMBER,"10","2","",discountRate1,"");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Discount_Amount1",NUMBER,"10","2","",discountAmount1,"");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Discount_Item_Desc_2",STRING,"25","",discountItemDesc2,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Discount_Item_Code2",STRING,"4","",discountItemCode2,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Discountable_Value2",NUMBER,"10","2","",discountableValue2,"");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Discount_Rate2",NUMBER,"10","2","",discountRate2,"");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Discount_Amount2",NUMBER,"10","2","",discountAmount2,"");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Discount_Item_Desc_3",STRING,"25","",discountItemDesc3,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Discount_Item_Code3",STRING,"4","",discountItemCode3,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Discountable_Value3",NUMBER,"10","2","",discountableValue3,"");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Discount_Rate3",NUMBER,"10","2","",discountRate3,"");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Discount_Amount3",NUMBER,"10","2","",discountAmount3,"");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Total_Amount",NUMBER,"10","2","",totalAmount,"");
          writeToWork2File(workLine);
          recordSeqNoReports++;
        }
      }
      catch(java.sql.SQLException ex)
      {
        message = "   Error getting switched discount summary report details for conglom invoice "+invoiceNo+" : "+ex.toString();
        System.out.println(message);
        writeToLogFile(message);
        result = false;
      }
      finally
      {
        reportRS=null;
      }
    }
    return result;
  }

  private boolean conglomSwitchedSpecials(String globalCustomerId, String accountName, String invoiceNo)
  {
    boolean result = true;
    String rName = "cb_ssvo_specials";
    // supplementary line
    String workLine =
      supplementaryLine(
        bsName,
        workFileNo,
        recordSeqNoReports,
        globalCustomerId,
        accountName,
        invoiceNo,
        CHARGE,
        billDate,
        "0.00",
        "0.00",
        invoiceCurrency,
        invoiceCurrency,
        rName,
        "1.00",
        "",
        CONGLOM);
    writeToWork2File(workLine);
    recordSeqNoReports++;
    ResultSet reportRS = eDB.getReportDetails(invoiceNo,rName);
    try
    {
          while (reportRS.next())
      {
        String conglomCustName = tidyString(reportRS.getString("Conglom_Cust_Name"));
        String cwContactTitle = tidyString(reportRS.getString("CW_Contact_Title"));
        String cwContactName = tidyString(reportRS.getString("CW_Contact_Name"));
        String conglomInvoiceRef = tidyString(reportRS.getString("Conglom_Invoice_Ref"));
        String billDate = reformatConglomDate(reportRS.getString("Bill_Date"));
        String invoiceCurrency = tidyString(reportRS.getString("Invoice_Currency"));
        String sourceAccountNo = tidyString(reportRS.getString("Source_Account_No"));
        String sourceInvoiceNo = tidyString(reportRS.getString("Source_Invoice_No"));
        String billedDate = reformatConglomDate(reportRS.getString("Billed_Date"));
        String dateRaised = reformatConglomDate(reportRS.getString("Date_Raised"));
        String description = tidyString(reportRS.getString("Description"));
        String netAmount = tidyString(reportRS.getString("Net_Amount"));
        // supplementary detail
        workLine =
          supplementaryDetail(
            bsName,
            workFileNo,
            recordSeqNoReports,
            globalCustomerId,
            accountName,
            invoiceNo,
            CHARGE,
            billDate,
            "0.00",
            "0.00",
            invoiceCurrency,
            invoiceCurrency,
            rName,
            "1.00",
            "",
            CONGLOM);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        // source data
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Conglom_Cust_Name",STRING,"50","",conglomCustName,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "CW_Contact_Title",STRING,"31","",cwContactTitle,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "CW_Contact_Name",STRING,"30","",cwContactName,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Conglom_Invoice_Ref",STRING,"13","",conglomInvoiceRef,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Bill_Date",DATE,"","","","",billDate);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Invoice_Currency",STRING,"3","",invoiceCurrency,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Source_Account_No",STRING,"10","",sourceAccountNo,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Source_Invoice_No",STRING,"10","",sourceInvoiceNo,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Billed_Date",DATE,"","","","",billedDate);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Date_Raised",DATE,"","","","",dateRaised);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Description",STRING,"50","",description,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Net_Amount",NUMBER,"10","2","",netAmount,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
      }
    }
    catch(java.sql.SQLException ex)
    {
      message = "   Error getting switched specials report details for conglom invoice "+invoiceNo+" : "+ex.toString();
      System.out.println(message);
      writeToLogFile(message);
      result = false;
    }
    finally
    {
      reportRS=null;
    }
    return result;
  }

  private boolean conglomSwitchedEasyQuarterly(String globalCustomerId, String accountName, String invoiceNo)
  {
    boolean result = true;
    String rName = "cb_ssvo_easy_quarterly";
    // supplementary line
    String workLine =
      supplementaryLine(
        bsName,
        workFileNo,
        recordSeqNoReports,
        globalCustomerId,
        accountName,
        invoiceNo,
        CHARGE,
        billDate,
        "0.00",
        "0.00",
        invoiceCurrency,
        invoiceCurrency,
        rName,
        "1.00",
        "",
        CONGLOM);
    writeToWork2File(workLine);
    recordSeqNoReports++;
    ResultSet reportRS = eDB.getReportDetails(invoiceNo,rName);
    try
    {
          while (reportRS.next())
      {
        String conglomCustName = tidyString(reportRS.getString("Conglom_Cust_Name"));
        String cwContactTitle = tidyString(reportRS.getString("CW_Contact_Title"));
        String cwContactName = tidyString(reportRS.getString("CW_Contact_Name"));
        String conglomInvoiceRef = tidyString(reportRS.getString("Conglom_Invoice_Ref"));
        String billDate = reformatConglomDate(reportRS.getString("Bill_Date"));
        String invoiceCurrency = tidyString(reportRS.getString("Invoice_Currency"));
        //String billPeriodRef = tidyString(reportRS.getString("Bill_Period_Ref"));
        String sourceAccountNo = tidyString(reportRS.getString("Source_Account_no"));
        String sourceInvoiceNo = tidyString(reportRS.getString("Source_Invoice_No"));
        String billedDate = reformatConglomDate(reportRS.getString("Billed_Date"));
        String qtrlyChargeDate = reformatConglomDate(reportRS.getString("Qtrly_Charge_Date"));
        String qtrlyCharge = tidyString(reportRS.getString("Qtrly_Charge"));
        String qtrlyDiscountDate = reformatConglomDate(reportRS.getString("Qtrly_Discount_Date"));
        String qtrlyDiscountAmount = tidyString(reportRS.getString("Qtrly_Discount_Amount"));
        // supplementary detail
        workLine =
          supplementaryDetail(
            bsName,
            workFileNo,
            recordSeqNoReports,
            globalCustomerId,
            accountName,
            invoiceNo,
            CHARGE,
            billDate,
            "0.00",
            "0.00",
            invoiceCurrency,
            invoiceCurrency,
            rName,
            "1.00",
            "",
            CONGLOM);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        // source data
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Conglom_Cust_Name",STRING,"50","",conglomCustName,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "CW_Contact_Title",STRING,"31","",cwContactTitle,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "CW_Contact_Name",STRING,"30","",cwContactName,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Conglom_Invoice_Ref",STRING,"13","",conglomInvoiceRef,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Bill_Date",DATE,"","","","",billDate);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Invoice_Currency",STRING,"3","",invoiceCurrency,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        //workLine =
        //  sourceData(
        //    bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
        //    "Bill_Period_Ref",STRING,"4","",billPeriodRef,"","");
        //writeToWork2File(workLine);
        //recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Source_Account_No",STRING,"10","",sourceAccountNo,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Source_Invoice_No",STRING,"10","",sourceInvoiceNo,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Billed_Date",DATE,"","","","",billedDate);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Qtrly_Charge_Date",DATE,"","","","",qtrlyChargeDate);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Qtrly_Charge",NUMBER,"10","2","",qtrlyCharge,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Qtrly_Discount_Date",DATE,"","","","",qtrlyDiscountDate);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Qtrly_Discount_Amount",NUMBER,"10","2","",qtrlyDiscountAmount,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
      }
    }
    catch(java.sql.SQLException ex)
    {
      message = "   Error getting switched easy quarterly report details for conglom invoice "+invoiceNo+" : "+ex.toString();
      System.out.println(message);
      writeToLogFile(message);
      result = false;
    }
    finally
    {
      reportRS=null;
    }
    return result;
  }

  private boolean conglomSwitchedAuthcodeCharges(String globalCustomerId, String accountName, String invoiceNo)
  {
    boolean result = true;
    String rName = "cb_ssvo_authcode_charges";
    // supplementary line
    String workLine =
      supplementaryLine(
        bsName,
        workFileNo,
        recordSeqNoReports,
        globalCustomerId,
        accountName,
        invoiceNo,
        CHARGE,
        billDate,
        "0.00",
        "0.00",
        invoiceCurrency,
        invoiceCurrency,
        rName,
        "1.00",
        "",
        CONGLOM);
    writeToWork2File(workLine);
    recordSeqNoReports++;
    ResultSet reportRS = eDB.getReportDetails(invoiceNo,rName);
    try
    {
          while (reportRS.next())
      {
        String conglomCustName = tidyString(reportRS.getString("Conglom_Cust_Name"));
        String cwContactTitle = tidyString(reportRS.getString("CW_Contact_Title"));
        String cwContactName = tidyString(reportRS.getString("CW_Contact_Name"));
        String conglomInvoiceRef = tidyString(reportRS.getString("Conglom_Invoice_Ref"));
        String billDate = reformatConglomDate(reportRS.getString("Bill_Date"));
        String invoiceCurrency = tidyString(reportRS.getString("Invoice_Currency"));
        String billPeriodRef = tidyString(reportRS.getString("Bill_Period_Ref"));
        String sourceAccountNo = tidyString(reportRS.getString("Source_Account_No"));
        String sourceInvoiceNo = tidyString(reportRS.getString("Source_Invoice_No"));
        String billedDate = reformatConglomDate(reportRS.getString("Billed_Date"));
        String chargeDate = reformatConglomDate(reportRS.getString("Charge_Date"));
        String chargeDescription = tidyString(reportRS.getString("Charge_Description"));
        String chargeAmount = tidyString(reportRS.getString("Charge_Amount"));
        // supplementary detail
        workLine =
          supplementaryDetail(
            bsName,
            workFileNo,
            recordSeqNoReports,
            globalCustomerId,
            accountName,
            invoiceNo,
            CHARGE,
            billDate,
            "0.00",
            "0.00",
            invoiceCurrency,
            invoiceCurrency,
            rName,
            "1.00",
            "",
            CONGLOM);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        // source data
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Conglom_Cust_Name",STRING,"50","",conglomCustName,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "CW_Contact_Title",STRING,"31","",cwContactTitle,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "CW_Contact_Name",STRING,"30","",cwContactName,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Conglom_Invoice_Ref",STRING,"13","",conglomInvoiceRef,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Bill_Date",DATE,"","","","",billDate);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Invoice_Currency",STRING,"3","",invoiceCurrency,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Bill_Period_Ref",STRING,"4","",billPeriodRef,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Source_Account_No",STRING,"10","",sourceAccountNo,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Source_Invoice_No",STRING,"10","",sourceInvoiceNo,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Billed_Date",DATE,"","","","",billedDate);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Charge_Date",DATE,"","","","",chargeDate);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Charge_Description",STRING,"250","",chargeDescription,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Charge_Amount",NUMBER,"10","2","",chargeAmount,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
      }
    }
    catch(java.sql.SQLException ex)
    {
      message = "   Error getting switched authcode charges report details for conglom invoice "+invoiceNo+" : "+ex.toString();
      System.out.println(message);
      writeToLogFile(message);
      result = false;
    }
    finally
    {
      reportRS=null;
    }
    return result;
  }

  private boolean conglomLeasedAdjustments(String globalCustomerId, String accountName, String invoiceNo)
  {
    boolean result = true;
    String rName = "cb_pcbl_adjustments";
    // supplementary line
    String workLine =
      supplementaryLine(
        bsName,
        workFileNo,
        recordSeqNoReports,
        globalCustomerId,
        accountName,
        invoiceNo,
        CHARGE,
        billDate,
        "0.00",
        "0.00",
        invoiceCurrency,
        invoiceCurrency,
        rName,
        "1.00",
        "",
        CONGLOM);
    writeToWork2File(workLine);
    recordSeqNoReports++;
    ResultSet reportRS = eDB.getReportDetails(invoiceNo,rName);
    try
    {
          while (reportRS.next())
      {
        String conglomCustName = tidyString(reportRS.getString("Conglom_Cust_Name"));
        String cwContactTitle = tidyString(reportRS.getString("CW_Contact_Title"));
        String cwContactName = tidyString(reportRS.getString("CW_Contact_Name"));
        String conglomInvoiceRef = tidyString(reportRS.getString("Conglom_Invoice_Ref"));
        String billDate = reformatConglomDate(reportRS.getString("Bill_Date"));
        String invoiceCurrency = tidyString(reportRS.getString("Invoice_Currency"));
        String billPeriodRef = tidyString(reportRS.getString("Bill_Period_Ref"));
        String sourceAccountNo = tidyString(reportRS.getString("Source_Account_No"));
        String sourceInvoiceNo = tidyString(reportRS.getString("Source_Invoice_No"));
        String billedDate = reformatConglomDate(reportRS.getString("Billed_Date"));
        String dateRaised = reformatConglomDate(reportRS.getString("Date_Raised"));
        String docketNumber = tidyString(reportRS.getString("Docket_Number"));
        String description = tidyString(reportRS.getString("Description"));
        String netAmount = tidyString(reportRS.getString("Net_Amount"));
        // supplementary detail
        workLine =
          supplementaryDetail(
            bsName,
            workFileNo,
            recordSeqNoReports,
            globalCustomerId,
            accountName,
            invoiceNo,
            CHARGE,
            billDate,
            "0.00",
            "0.00",
            invoiceCurrency,
            invoiceCurrency,
            rName,
            "1.00",
            "",
            CONGLOM);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        // source data
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Conglom_Cust_Name",STRING,"50","",conglomCustName,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "CW_Contact_Title",STRING,"31","",cwContactTitle,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "CW_Contact_Name",STRING,"30","",cwContactName,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Conglom_Invoice_Ref",STRING,"13","",conglomInvoiceRef,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Bill_Date",DATE,"","","","",billDate);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Invoice_Currency",STRING,"3","",invoiceCurrency,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Bill_Period_Ref",STRING,"4","",billPeriodRef,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Source_Account_No",STRING,"10","",sourceAccountNo,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Source_Invoice_No",STRING,"10","",sourceInvoiceNo,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Billed_Date",DATE,"","","","",billedDate);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Date_Raised",DATE,"","","","",dateRaised);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Docket_Number",STRING,"10","",docketNumber,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Description",STRING,"50","",description,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Net_Amount",NUMBER,"10","2","",netAmount,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
      }
    }
    catch(java.sql.SQLException ex)
    {
      message = "   Error getting leased adjustments report details for conglom invoice "+invoiceNo+" : "+ex.toString();
      System.out.println(message);
      writeToLogFile(message);
      result = false;
    }
    finally
    {
      reportRS=null;
    }
    return result;
  }

  private boolean conglomLeasedRentals(String globalCustomerId, String accountName, String invoiceNo)
  {
    boolean result = true;
    String rName = "cb_pcbl_rentals";
    // supplementary line
    String workLine =
      supplementaryLine(
        bsName,
        workFileNo,
        recordSeqNoReports,
        globalCustomerId,
        accountName,
        invoiceNo,
        CHARGE,
        billDate,
        "0.00",
        "0.00",
        invoiceCurrency,
        invoiceCurrency,
        rName,
        "1.00",
        "",
        CONGLOM);
    writeToWork2File(workLine);
    recordSeqNoReports++;
    ResultSet reportRS = eDB.getReportDetails(invoiceNo,rName);
    try
    {
          while (reportRS.next())
      {
        String conglomCustName = tidyString(reportRS.getString("Conglom_Cust_Name"));
        String cwContactTitle = tidyString(reportRS.getString("CW_Contact_Title"));
        String cwContactName = tidyString(reportRS.getString("CW_Contact_Name"));
        String conglomInvoiceRef = tidyString(reportRS.getString("Conglom_Invoice_Ref"));
        String billDate = reformatConglomDate(reportRS.getString("Bill_Date"));
        String invoiceCurrency = tidyString(reportRS.getString("Invoice_Currency"));
        String billPeriodRef = tidyString(reportRS.getString("Bill_Period_Ref"));
        String sourceAccountNo = tidyString(reportRS.getString("Source_Account_No"));
        String sourceInvoiceNo = tidyString(reportRS.getString("Source_Invoice_No"));
        String billedDate = reformatConglomDate(reportRS.getString("Billed_Date"));
        String periodStartDate = reformatConglomDate(reportRS.getString("Period_Start_Date"));
        String periodEndDate = reformatConglomDate(reportRS.getString("Period_End_Date"));
        String circuitDescription = tidyString(reportRS.getString("Circuit_Description"));
        String aEnd = tidyString(reportRS.getString("A_End"));
        String bEnd = tidyString(reportRS.getString("B_End"));
        String circuitLength = tidyString(reportRS.getString("Circuit_Length"));
        String netAmount = tidyString(reportRS.getString("Net_Amount"));
        String vatAmount = tidyString(reportRS.getString("VAT_Amount"));
        String totalAmount = tidyString(reportRS.getString("Total_Amount"));
        String circuitReference = tidyString(reportRS.getString("Circuit_Reference"));
        String contractNumber = tidyString(reportRS.getString("Contract_Number"));
        // supplementary detail
        workLine =
          supplementaryDetail(
            bsName,
            workFileNo,
            recordSeqNoReports,
            globalCustomerId,
            accountName,
            invoiceNo,
            CHARGE,
            billDate,
            "0.00",
            "0.00",
            invoiceCurrency,
            invoiceCurrency,
            rName,
            "1.00",
            "",
            CONGLOM);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        // source data
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Conglom_Cust_Name",STRING,"50","",conglomCustName,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "CW_Contact_Title",STRING,"31","",cwContactTitle,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "CW_Contact_Name",STRING,"30","",cwContactName,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Conglom_Invoice_Ref",STRING,"13","",conglomInvoiceRef,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Bill_Date",DATE,"","","","",billDate);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Invoice_Currency",STRING,"3","",invoiceCurrency,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Bill_Period_Ref",STRING,"4","",billPeriodRef,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Source_Account_No",STRING,"10","",sourceAccountNo,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Source_Invoice_No",STRING,"10","",sourceInvoiceNo,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Billed_Date",DATE,"","","","",billedDate);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Period_Start_Date",DATE,"","","","",periodStartDate);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Period_End_Date",DATE,"","","","",periodEndDate);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Circuit_Description",STRING,"50","",circuitDescription,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "A_End",STRING,"160","",aEnd,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "B_End",STRING,"160","",bEnd,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Circuit_Length",NUMBER,"10","2","",circuitLength,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Net_Amount",NUMBER,"10","2","",netAmount,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "VAT_Amount",NUMBER,"10","2","",vatAmount,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Total_Amount",NUMBER,"10","2","",totalAmount,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Circuit_Reference",STRING,"15","",circuitReference,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Contract_Number",STRING,"20","",contractNumber,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
      }
    }
    catch(java.sql.SQLException ex)
    {
      message = "   Error getting leased rentals report details for conglom invoice "+invoiceNo+" : "+ex.toString();
      System.out.println(message);
      writeToLogFile(message);
      result = false;
    }
    finally
    {
      reportRS=null;
    }
    return result;
  }


  private boolean conglomLeasedSundry(String globalCustomerId, String accountName, String invoiceNo)
  {
    boolean result = true;
    String rName = "cb_pcbl_sundry";
    // supplementary line
    String workLine =
      supplementaryLine(
        bsName,
        workFileNo,
        recordSeqNoReports,
        globalCustomerId,
        accountName,
        invoiceNo,
        CHARGE,
        billDate,
        "0.00",
        "0.00",
        invoiceCurrency,
        invoiceCurrency,
        rName,
        "1.00",
        "",
        CONGLOM);
    writeToWork2File(workLine);
    recordSeqNoReports++;
    ResultSet reportRS = eDB.getReportDetails(invoiceNo,rName);
    try
    {
          while (reportRS.next())
      {
        String conglomCustName = tidyString(reportRS.getString("Conglom_Cust_Name"));
        String cwContactTitle = tidyString(reportRS.getString("CW_Contact_Title"));
        String cwContactName = tidyString(reportRS.getString("CW_Contact_Name"));
        String conglomInvoiceRef = tidyString(reportRS.getString("Conglom_Invoice_Ref"));
        String billDate = reformatConglomDate(reportRS.getString("Bill_Date"));
        String invoiceCurrency = tidyString(reportRS.getString("Invoice_Currency"));
        String billPeriodRef = tidyString(reportRS.getString("Bill_Period_Ref"));
        String sourceAccountNo = tidyString(reportRS.getString("Source_Account_No"));
        String sourceInvoiceNo = tidyString(reportRS.getString("Source_Invoice_No"));
        String billedDate = reformatConglomDate(reportRS.getString("Billed_Date"));
        String rName2 = tidyString(reportRS.getString("R_Name"));
        String description = tidyString(reportRS.getString("Description"));
        String netAmount = tidyString(reportRS.getString("Net_Amount"));
        // supplementary detail
        workLine =
          supplementaryDetail(
            bsName,
            workFileNo,
            recordSeqNoReports,
            globalCustomerId,
            accountName,
            invoiceNo,
            CHARGE,
            billDate,
            "0.00",
            "0.00",
            invoiceCurrency,
            invoiceCurrency,
            rName,
            "1.00",
            "",
            CONGLOM);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        // source data
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Conglom_Cust_Name",STRING,"50","",conglomCustName,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "CW_Contact_Title",STRING,"31","",cwContactTitle,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "CW_Contact_Name",STRING,"30","",cwContactName,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Conglom_Invoice_Ref",STRING,"13","",conglomInvoiceRef,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Bill_Date",DATE,"","","","",billDate);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Invoice_Currency",STRING,"3","",invoiceCurrency,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Bill_Period_Ref",STRING,"4","",billPeriodRef,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Source_Account_No",STRING,"10","",sourceAccountNo,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Source_Invoice_No",STRING,"10","",sourceInvoiceNo,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Billed_Date",DATE,"","","","",billedDate);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "R_Name",STRING,"10","",rName2,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Description",STRING,"10","",description,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Net_Amount",NUMBER,"10","2","",netAmount,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
      }
    }
    catch(java.sql.SQLException ex)
    {
      message = "   Error getting leased sundry report details for conglom invoice "+invoiceNo+" : "+ex.toString();
      System.out.println(message);
      writeToLogFile(message);
      result = false;
    }
    finally
    {
      reportRS=null;
    }
    return result;
  }

  private boolean conglomLeasedInstalls(String globalCustomerId, String accountName, String invoiceNo)
  {
    boolean result = true;
    String rName = "cb_pcbl_installs";
    // supplementary line
    String workLine =
      supplementaryLine(
        bsName,
        workFileNo,
        recordSeqNoReports,
        globalCustomerId,
        accountName,
        invoiceNo,
        CHARGE,
        billDate,
        "0.00",
        "0.00",
        invoiceCurrency,
        invoiceCurrency,
        rName,
        "1.00",
        "",
        CONGLOM);
    writeToWork2File(workLine);
    recordSeqNoReports++;
    ResultSet reportRS = eDB.getReportDetails(invoiceNo,rName);
    try
    {
          while (reportRS.next())
      {
        String conglomCustName = tidyString(reportRS.getString("Conglom_Cust_Name"));
        String cwContactTitle = tidyString(reportRS.getString("CW_Contact_Title"));
        String cwContactName = tidyString(reportRS.getString("CW_Contact_Name"));
        String conglomInvoiceRef = tidyString(reportRS.getString("Conglom_Invoice_Ref"));
        String billDate = reformatConglomDate(reportRS.getString("Bill_Date"));
        String invoiceCurrency = tidyString(reportRS.getString("Invoice_Currency"));
        String billPeriodRef = tidyString(reportRS.getString("Bill_Period_Ref"));
        String sourceAccountNo = tidyString(reportRS.getString("Source_Account_No"));
        String sourceInvoiceNo = tidyString(reportRS.getString("Source_Invoice_No"));
        String billedDate = reformatConglomDate(reportRS.getString("Billed_Date"));
        String chargeDate = reformatConglomDate(reportRS.getString("Charge_Date"));
        String contractNumber = tidyString(reportRS.getString("Contract_Number"));
        String circuitReference = tidyString(reportRS.getString("Circuit_Reference"));
        String chargeDescription = tidyString(reportRS.getString("Charge_Description"));
        String chargeQuantity = tidyString(reportRS.getString("Charge_Quantity"));
        String chargeAmount = tidyString(reportRS.getString("Charge_Amount"));
        // supplementary detail
        workLine =
          supplementaryDetail(
            bsName,
            workFileNo,
            recordSeqNoReports,
            globalCustomerId,
            accountName,
            invoiceNo,
            CHARGE,
            billDate,
            "0.00",
            "0.00",
            invoiceCurrency,
            invoiceCurrency,
            rName,
            "1.00",
            "",
            CONGLOM);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        // source data
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Conglom_Cust_Name",STRING,"50","",conglomCustName,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "CW_Contact_Title",STRING,"31","",cwContactTitle,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "CW_Contact_Name",STRING,"30","",cwContactName,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Conglom_Invoice_Ref",STRING,"13","",conglomInvoiceRef,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Bill_Date",DATE,"","","","",billDate);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Invoice_Currency",STRING,"3","",invoiceCurrency,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Bill_Period_Ref",STRING,"4","",billPeriodRef,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Source_Account_No",STRING,"10","",sourceAccountNo,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Source_Invoice_No",STRING,"10","",sourceInvoiceNo,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Billed_Date",DATE,"","","","",billedDate);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Charge_Date",DATE,"","","","",chargeDate);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Contract_Number",STRING,"20","",contractNumber,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Circuit_Ref",STRING,"15","",circuitReference,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Charge_Description",STRING,"50","",chargeDescription,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Charge_Quantity",STRING,"10","0","",chargeQuantity,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Charge_Amount",STRING,"10","2","",chargeAmount,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
      }
    }
    catch(java.sql.SQLException ex)
    {
      message = "   Error getting leased installs report details for conglom invoice "+invoiceNo+" : "+ex.toString();
      System.out.println(message);
      writeToLogFile(message);
      result = false;
    }
    finally
    {
      reportRS=null;
    }
    return result;
  }

  private boolean conglomLeasedConsolNOLD(String globalCustomerId, String accountName, String invoiceNo)
  {
    boolean result = true;
    String rName = "cb_pcbl_consolidated_nold";
    // supplementary line
    String workLine =
      supplementaryLine(
        bsName,
        workFileNo,
        recordSeqNoReports,
        globalCustomerId,
        accountName,
        invoiceNo,
        CHARGE,
        billDate,
        "0.00",
        "0.00",
        invoiceCurrency,
        invoiceCurrency,
        rName,
        "1.00",
        "",
        CONGLOM);
    writeToWork2File(workLine);
    recordSeqNoReports++;
    ResultSet reportRS = eDB.getReportDetails(invoiceNo,rName);
    try
    {
          while (reportRS.next())
      {
        String subTotalLevel1 = tidyString(reportRS.getString("Sub_Total_Level1"));
        String subTotalLevel2 = tidyString(reportRS.getString("Sub_Total_Level2"));
        String subtField1 = tidyString(reportRS.getString("SubT_Field_1"));
        String subtField2 = tidyString(reportRS.getString("SubT_Field_2"));
        String billedProductId = tidyString(reportRS.getString("Billed_Product_Id"));
        String description = tidyString(reportRS.getString("Description"));
        String conglomCustName = tidyString(reportRS.getString("Conglom_Cust_Name"));
        String cwContactTitle = tidyString(reportRS.getString("CW_Contact_Title"));
        String cwContactName = tidyString(reportRS.getString("CW_Contact_Name"));
        String conglomInvoiceRef = tidyString(reportRS.getString("Conglom_Invoice_Ref"));
        String billDate = reformatConglomDate(reportRS.getString("Bill_Date"));
        String invoiceCurrency = tidyString(reportRS.getString("Invoice_Currency"));
        String billPeriodRef = tidyString(reportRS.getString("Bill_Period_Ref"));
        String sourceAccountNo = tidyString(reportRS.getString("Source_Account_No"));
        String sourceInvoiceNo = tidyString(reportRS.getString("Source_Invoice_No"));
        String billedDate = reformatConglomDate(reportRS.getString("Billed_Date"));
        String oneOffCharges = tidyString(reportRS.getString("One_Off_Charges"));
        String recurringCharges = tidyString(reportRS.getString("Recurring_Charges"));
        String usageCharges = tidyString(reportRS.getString("Usage_Charges"));
        String miscCharges = tidyString(reportRS.getString("Misc_Charges"));
        String adjustmentTotal = tidyString(reportRS.getString("Adjustment_Total"));
        String sourceDiscTotal = tidyString(reportRS.getString("Source_Disc_Total"));
        String conglomDiscTotal = tidyString(reportRS.getString("Conglom_Disc_Total"));
        String acctBalNetAmount = tidyString(reportRS.getString("Acct_Bal_Net_Amount"));
        String acctBalVatAmount = tidyString(reportRS.getString("Acct_Bal_Vat_Amount"));
        String acctBalAmount = tidyString(reportRS.getString("Acct_Bal_Amount"));
        // supplementary detail
        workLine =
          supplementaryDetail(
            bsName,
            workFileNo,
            recordSeqNoReports,
            globalCustomerId,
            accountName,
            invoiceNo,
            CHARGE,
            billDate,
            "0.00",
            "0.00",
            invoiceCurrency,
            invoiceCurrency,
            rName,
            "1.00",
            "",
            CONGLOM);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        // source data
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Sub_Total_Level1",STRING,"3","",subTotalLevel1,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Sub_Total_Level2",STRING,"3","",subTotalLevel2,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "SubT_Field_1",STRING,"10","",subtField1,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "SubT_Field_2",STRING,"10","",subtField2,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Billed_Product_Id",STRING,"4","",billedProductId,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Description",STRING,"50","",description,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Conglom_Cust_Name",STRING,"50","",conglomCustName,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "CW_Contact_Title",STRING,"31","",cwContactTitle,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "CW_Contact_Name",STRING,"30","",cwContactName,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Conglom_Invoice_Ref",STRING,"13","",conglomInvoiceRef,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Bill_Date",DATE,"","","","",billDate);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Invoice_Currency",STRING,"3","",invoiceCurrency,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Bill_Period_Ref",STRING,"4","",billPeriodRef,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Source_Account_No",STRING,"10","",sourceAccountNo,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Source_Invoice_No",STRING,"10","",sourceInvoiceNo,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Billed_Date",DATE,"","","","",billedDate);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "One_Off_Charges",NUMBER,"","","",oneOffCharges,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Recurring_Charges",NUMBER,"","","",recurringCharges,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Usage_Charges",NUMBER,"","","",usageCharges,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Misc_Charges",NUMBER,"","","",miscCharges,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Adjustment_Total",NUMBER,"","","",adjustmentTotal,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Source_Disc_Total",NUMBER,"","","",sourceDiscTotal,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Conglom_Disc_Total",NUMBER,"","","",conglomDiscTotal,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Acct_Bal_Net_Amount",NUMBER,"","","",acctBalNetAmount,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Acct_Bal_Vat_Amount",NUMBER,"","","",acctBalVatAmount,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Acct_Bal_Amount",NUMBER,"","","",acctBalAmount,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
      }
    }
    catch(java.sql.SQLException ex)
    {
      message = "   Error getting leased consolidated NOLD report details for conglom invoice "+invoiceNo+" : "+ex.toString();
      System.out.println(message);
      writeToLogFile(message);
      result = false;
    }
    finally
    {
      reportRS=null;
    }
    return result;
  }

  private boolean conglomLeasedConsolLD(String globalCustomerId, String accountName, String invoiceNo)
  {
    boolean result = true;
    String rName = "cb_pcbl_consolidated_subt";
    // supplementary line
    String workLine =
      supplementaryLine(
        bsName,
        workFileNo,
        recordSeqNoReports,
        globalCustomerId,
        accountName,
        invoiceNo,
        CHARGE,
        billDate,
        "0.00",
        "0.00",
        invoiceCurrency,
        invoiceCurrency,
        rName,
        "1.00",
        "",
        CONGLOM);
    writeToWork2File(workLine);
    recordSeqNoReports++;
    if (eDB.getLDCount(invoiceNo,CONSOL,PCBL)>0)
    {
      ResultSet reportRS = eDB.getReportDetails(invoiceNo,rName);
      try
      {
            while (reportRS.next())
        {
          String subTotalLevel1 = tidyString(reportRS.getString("Sub_Total_Level1"));
          String subTotalLevel2 = tidyString(reportRS.getString("Sub_Total_Level2"));
          String subtField1 = tidyString(reportRS.getString("SubT_Field_1"));
          String subtField2 = tidyString(reportRS.getString("SubT_Field_2"));
          String conglomCustName = tidyString(reportRS.getString("Conglom_Cust_Name"));
          String cwContactTitle = tidyString(reportRS.getString("CW_Contact_Title"));
          String cwContactName = tidyString(reportRS.getString("CW_Contact_Name"));
          String conglomInvoiceRef = tidyString(reportRS.getString("Conglom_Invoice_Ref"));
          String billDate = reformatConglomDate(reportRS.getString("Bill_Date"));
          String invoiceCurrency = tidyString(reportRS.getString("Invoice_Currency"));
          String billPeriodRef = tidyString(reportRS.getString("Bill_Period_Ref"));
          String sourceAccountNo = tidyString(reportRS.getString("Source_Account_No"));
          String sourceInvoiceNo = tidyString(reportRS.getString("Source_Invoice_No"));
          String billedDate = reformatConglomDate(reportRS.getString("Billed_Date"));
          String oneOffCharges = tidyString(reportRS.getString("One_Off_Charges"));
          String recurringCharges = tidyString(reportRS.getString("Recurring_Charges"));
          String usageCharges = tidyString(reportRS.getString("Usage_Charges"));
          String miscCharges = tidyString(reportRS.getString("Misc_Charges"));
          String adjustmentTotal = tidyString(reportRS.getString("Adjustment_Total"));
          String sourceDiscTotal = tidyString(reportRS.getString("Source_Disc_Total"));
          String conglomDiscTotal = tidyString(reportRS.getString("Conglom_Disc_Total"));
          String acctBalNetAmount = tidyString(reportRS.getString("Acct_Bal_Net_Amount"));
          String acctBalVatAmount = tidyString(reportRS.getString("Acct_Bal_Vat_Amount"));
          String acctBalAmount = tidyString(reportRS.getString("Acct_Bal_Amount"));
          String ldHeader1 = reportRS.getString("LD_Header1");
          String ldHeader2 = reportRS.getString("LD_Header2");
          String ldHeader3 = reportRS.getString("LD_Header3");
          String reportLDString = tidyString2(reportRS.getString("Report_LD_String"));
          // supplementary detail
          workLine =
            supplementaryDetail(
              bsName,
              workFileNo,
              recordSeqNoReports,
              globalCustomerId,
              accountName,
              invoiceNo,
              CHARGE,
              billDate,
              "0.00",
              "0.00",
              invoiceCurrency,
              invoiceCurrency,
              rName,
              "1.00",
              "",
              CONGLOM);
          writeToWork2File(workLine);
          recordSeqNoReports++;
          // source data
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Sub_Total_Level1",STRING,"3","",subTotalLevel1,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Sub_Total_Level2",STRING,"3","",subTotalLevel2,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "SubT_Field_1",STRING,"10","",subtField1,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "SubT_Field_2",STRING,"10","",subtField2,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Conglom_Cust_Name",STRING,"50","",conglomCustName,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "CW_Contact_Title",STRING,"31","",cwContactTitle,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "CW_Contact_Name",STRING,"30","",cwContactName,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Conglom_Invoice_Ref",STRING,"13","",conglomInvoiceRef,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Bill_Date",DATE,"","","","",billDate);
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Invoice_Currency",STRING,"3","",invoiceCurrency,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Bill_Period_Ref",STRING,"4","",billPeriodRef,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Source_Account_No",STRING,"10","",sourceAccountNo,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Source_Invoice_No",STRING,"10","",sourceInvoiceNo,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Billed_Date",DATE,"","","","",billedDate);
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "One_Off_Charges",NUMBER,"","","",oneOffCharges,"");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Recurring_Charges",NUMBER,"","","",recurringCharges,"");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Usage_Charges",NUMBER,"","","",usageCharges,"");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Misc_Charges",NUMBER,"","","",miscCharges,"");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Adjustment_Total",NUMBER,"","","",adjustmentTotal,"");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Source_Disc_Total",NUMBER,"","","",sourceDiscTotal,"");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Conglom_Disc_Total",NUMBER,"","","",conglomDiscTotal,"");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Acct_Bal_Net_Amount",NUMBER,"","","",acctBalNetAmount,"");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Acct_Bal_Vat_Amount",NUMBER,"","","",acctBalVatAmount,"");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Acct_Bal_Amount",NUMBER,"","","",acctBalAmount,"");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "LD_Header1",STRING,"70","",ldHeader1,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "LD_Header2",STRING,"70","",ldHeader2,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "LD_Header3",STRING,"70","",ldHeader3,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Report_LD_String",STRING,"70","",reportLDString,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
        }
      }
      catch(java.sql.SQLException ex)
      {
        message = "   Error getting leased consolidated LD report details for conglom invoice "+invoiceNo+" : "+ex.toString();
        System.out.println(message);
        writeToLogFile(message);
        result = false;
      }
      finally
      {
        reportRS=null;
      }
    }
    return result;
  }

  private boolean conglomSwitchedUsageNOLD(String globalCustomerId, String accountName, String invoiceNo)
  {
    boolean result = true;
    String rName = "cb_ssvo_usage_nold";
    // supplementary line
    String workLine =
      supplementaryLine(
        bsName,
        workFileNo,
        recordSeqNoReports,
        globalCustomerId,
        accountName,
        invoiceNo,
        CHARGE,
        billDate,
        "0.00",
        "0.00",
        invoiceCurrency,
        invoiceCurrency,
        rName,
        "1.00",
        "",
        CONGLOM);
    writeToWork2File(workLine);
    recordSeqNoReports++;
    ResultSet reportRS = eDB.getReportDetails(invoiceNo,rName);
    try
    {
          while (reportRS.next())
      {
        String subTotalLevel1 = tidyString(reportRS.getString("Sub_Total_Level1"));
        String subTotalLevel2 = tidyString(reportRS.getString("Sub_Total_Level2"));
        String subtField1 = tidyString(reportRS.getString("SubT_Field_1"));
        String subtField2 = tidyString(reportRS.getString("SubT_Field_2"));
        String conglomCustName = tidyString(reportRS.getString("Conglom_Cust_Name"));
        String cwContactTitle = tidyString(reportRS.getString("CW_Contact_Title"));
        String cwContactName = tidyString(reportRS.getString("CW_Contact_Name"));
        String conglomInvoiceRef = tidyString(reportRS.getString("Conglom_Invoice_Ref"));
        String billDate = reformatConglomDate(reportRS.getString("Bill_Date"));
        String invoiceCurrency = tidyString(reportRS.getString("Invoice_Currency"));
        String billPeriodRef = tidyString(reportRS.getString("Bill_Period_Ref"));
        String sourceAccountNo = tidyString(reportRS.getString("Source_Account_No"));
        String sourceInvoiceNo = tidyString(reportRS.getString("Source_Invoice_No"));
        String billedDate = reformatConglomDate(reportRS.getString("Billed_Date"));
        String callinkCharges = tidyString(reportRS.getString("Callink_Charges"));
        String vpnCharges = tidyString(reportRS.getString("VPN_Charges"));
        String callCharges = tidyString(reportRS.getString("Call_Charges"));
        String easyUsageCharges = tidyString(reportRS.getString("Easy_Usage_Charges"));
        String authCodeCharges = tidyString(reportRS.getString("AuthCode_Charges"));
        String usageCharges = tidyString(reportRS.getString("Usage_Charges"));
        // supplementary detail
        workLine =
          supplementaryDetail(
            bsName,
            workFileNo,
            recordSeqNoReports,
            globalCustomerId,
            accountName,
            invoiceNo,
            CHARGE,
            billDate,
            "0.00",
            "0.00",
            invoiceCurrency,
            invoiceCurrency,
            rName,
            "1.00",
            "",
            CONGLOM);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        // source data
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Sub_Total_Level1",STRING,"3","",subTotalLevel1,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Sub_Total_Level2",STRING,"3","",subTotalLevel2,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "SubT_Field_1",STRING,"10","",subtField1,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "SubT_Field_2",STRING,"10","",subtField2,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Conglom_Cust_Name",STRING,"50","",conglomCustName,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "CW_Contact_Title",STRING,"31","",cwContactTitle,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "CW_Contact_Name",STRING,"30","",cwContactName,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Conglom_Invoice_Ref",STRING,"13","",conglomInvoiceRef,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Bill_Date",DATE,"","","","",billDate);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Invoice_Currency",STRING,"3","",invoiceCurrency,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Bill_Period_Ref",STRING,"4","",billPeriodRef,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Source_Account_No",STRING,"10","",sourceAccountNo,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Source_Invoice_No",STRING,"10","",sourceInvoiceNo,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Billed_Date",DATE,"","","","",billedDate);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Callink_Charges",NUMBER,"10","2","",callinkCharges,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "VPN_Charges",NUMBER,"10","2","",vpnCharges,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Call_Charges",NUMBER,"10","2","",callCharges,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Easy_Usage_Charges",NUMBER,"10","2","",easyUsageCharges,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "AuthCode_Charges",NUMBER,"10","2","",authCodeCharges,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Usage_Charges",NUMBER,"10","2","",usageCharges,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
      }
    }
    catch(java.sql.SQLException ex)
    {
      message = "   Error getting switched usage NOLD report details for conglom invoice "+invoiceNo+" : "+ex.toString();
      System.out.println(message);
      writeToLogFile(message);
      result = false;
    }
    finally
    {
      reportRS=null;
    }
    return result;
  }

  private boolean conglomSwitchedUsageLD(String globalCustomerId, String accountName, String invoiceNo)
  {
    boolean result = true;
    String rName = "cb_ssvo_usage";
    // supplementary line
    String workLine =
      supplementaryLine(
        bsName,
        workFileNo,
        recordSeqNoReports,
        globalCustomerId,
        accountName,
        invoiceNo,
        CHARGE,
        billDate,
        "0.00",
        "0.00",
        invoiceCurrency,
        invoiceCurrency,
        rName,
        "1.00",
        "",
        CONGLOM);
    writeToWork2File(workLine);
    recordSeqNoReports++;
    if (eDB.getLDCount(invoiceNo,CONSOL,SSVO)>0)
    {
      ResultSet reportRS = eDB.getReportDetails(invoiceNo,rName);
      try
      {
            while (reportRS.next())
        {
          String subTotalLevel1 = tidyString(reportRS.getString("Sub_Total_Level1"));
          String subTotalLevel2 = tidyString(reportRS.getString("Sub_Total_Level2"));
          String subtField1 = tidyString(reportRS.getString("SubT_Field_1"));
          String subtField2 = tidyString(reportRS.getString("SubT_Field_2"));
          String conglomCustName = tidyString(reportRS.getString("Conglom_Cust_Name"));
          String cwContactTitle = tidyString(reportRS.getString("CW_Contact_Title"));
          String cwContactName = tidyString(reportRS.getString("CW_Contact_Name"));
          String conglomInvoiceRef = tidyString(reportRS.getString("Conglom_Invoice_Ref"));
          String billDate = reformatConglomDate(reportRS.getString("Bill_Date"));
          String invoiceCurrency = tidyString(reportRS.getString("Invoice_Currency"));
          String billPeriodRef = tidyString(reportRS.getString("Bill_Period_Ref"));
          String sourceAccountNo = tidyString(reportRS.getString("Source_Account_No"));
          String sourceInvoiceNo = tidyString(reportRS.getString("Source_Invoice_No"));
          String billedDate = reformatConglomDate(reportRS.getString("Billed_Date"));
          String callinkCharges = tidyString(reportRS.getString("Callink_Charges"));
          String vpnCharges = tidyString(reportRS.getString("VPN_Charges"));
          String callCharges = tidyString(reportRS.getString("Call_Charges"));
          String easyUsageCharges = tidyString(reportRS.getString("Easy_Usage_Charges"));
          String authCodeCharges = tidyString(reportRS.getString("AuthCode_Charges"));
          String usageCharges = tidyString(reportRS.getString("Usage_Charges"));
          String ldHeader1 = reportRS.getString("LD_Header1");
          String ldHeader2 = reportRS.getString("LD_Header2");
          String ldHeader3 = reportRS.getString("LD_Header3");
          String reportLDString = tidyString2(reportRS.getString("Report_LD_String"));
          // supplementary detail
          workLine =
            supplementaryDetail(
              bsName,
              workFileNo,
              recordSeqNoReports,
              globalCustomerId,
              accountName,
              invoiceNo,
              CHARGE,
              billDate,
              "0.00",
              "0.00",
              invoiceCurrency,
              invoiceCurrency,
              rName,
              "1.00",
              "",
              CONGLOM);
          writeToWork2File(workLine);
          recordSeqNoReports++;
          // source data
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Sub_Total_Level1",STRING,"3","",subTotalLevel1,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Sub_Total_Level2",STRING,"3","",subTotalLevel2,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "SubT_Field_1",STRING,"10","",subtField1,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "SubT_Field_2",STRING,"10","",subtField2,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Conglom_Cust_Name",STRING,"50","",conglomCustName,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "CW_Contact_Title",STRING,"31","",cwContactTitle,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "CW_Contact_Name",STRING,"30","",cwContactName,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Conglom_Invoice_Ref",STRING,"13","",conglomInvoiceRef,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Bill_Date",DATE,"","","","",billDate);
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Invoice_Currency",STRING,"3","",invoiceCurrency,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Bill_Period_Ref",STRING,"4","",billPeriodRef,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Source_Account_No",STRING,"10","",sourceAccountNo,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Source_Invoice_No",STRING,"10","",sourceInvoiceNo,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Billed_Date",DATE,"","","","",billedDate);
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Callink_Charges",NUMBER,"10","2","",callinkCharges,"");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "VPN_Charges",NUMBER,"10","2","",vpnCharges,"");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Call_Charges",NUMBER,"10","2","",callCharges,"");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Easy_Usage_Charges",NUMBER,"10","2","",easyUsageCharges,"");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "AuthCode_Charges",NUMBER,"10","2","",authCodeCharges,"");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Usage_Charges",NUMBER,"10","2","",usageCharges,"");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "LD_Header1",STRING,"70","",ldHeader1,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "LD_Header2",STRING,"70","",ldHeader2,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "LD_Header3",STRING,"70","",ldHeader3,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Report_LD_String",STRING,"50","",reportLDString,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
        }
      }
      catch(java.sql.SQLException ex)
      {
        message = "   Error getting switched usage LD report details for conglom invoice "+invoiceNo+" : "+ex.toString();
        System.out.println(message);
        writeToLogFile(message);
        result = false;
      }
      finally
      {
        reportRS=null;
      }
    }
    return result;
  }

  private boolean conglomSwitchedVPN(String globalCustomerId, String accountName, String invoiceNo)
  {
    boolean result = true;
    String rName = "cb_ssvo_vpn";
    // supplementary line
    String workLine =
      supplementaryLine(
        bsName,
        workFileNo,
        recordSeqNoReports,
        globalCustomerId,
        accountName,
        invoiceNo,
        CHARGE,
        billDate,
        "0.00",
        "0.00",
        invoiceCurrency,
        invoiceCurrency,
        rName,
        "1.00",
        "",
        CONGLOM);
    writeToWork2File(workLine);
    recordSeqNoReports++;
    ResultSet reportRS = eDB.getReportDetails(invoiceNo,rName);
    try
    {
          while (reportRS.next())
      {
        String conglomCustName = tidyString(reportRS.getString("Conglom_Cust_Name"));
        String cwContactTitle = tidyString(reportRS.getString("CW_Contact_Title"));
        String cwContactName = tidyString(reportRS.getString("CW_Contact_Name"));
        String conglomInvoiceRef = tidyString(reportRS.getString("Conglom_Invoice_Ref"));
        String billDate = reformatConglomDate(reportRS.getString("Bill_Date"));
        String invoiceCurrency = tidyString(reportRS.getString("Invoice_Currency"));
        String billPeriodRef = tidyString(reportRS.getString("Bill_Period_Ref"));
        String sourceAccountNo = tidyString(reportRS.getString("Source_Account_No"));
        String sourceInvoiceNo = tidyString(reportRS.getString("Source_Invoice_No"));
        String billedDate = reformatConglomDate(reportRS.getString("Billed_Date"));
        String currPeriodStartDate = reformatConglomDate(reportRS.getString("Curr_Period_Start_Date"));
        String currPeriodEndDate = reformatConglomDate(reportRS.getString("Curr_Period_End_Date"));
        String totalUsageCharges = tidyString(reportRS.getString("Total_Usage_Charges"));
        // supplementary detail
        workLine =
          supplementaryDetail(
            bsName,
            workFileNo,
            recordSeqNoReports,
            globalCustomerId,
            accountName,
            invoiceNo,
            CHARGE,
            billDate,
            "0.00",
            "0.00",
            invoiceCurrency,
            invoiceCurrency,
            rName,
            "1.00",
            "",
            CONGLOM);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        // source data
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Conglom_Cust_Name",STRING,"50","",conglomCustName,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "CW_Contact_Title",STRING,"31","",cwContactTitle,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "CW_Contact_Name",STRING,"30","",cwContactName,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Conglom_Invoice_Ref",STRING,"13","",conglomInvoiceRef,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Bill_Date",DATE,"","","","",billDate);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Invoice_Currency",STRING,"3","",invoiceCurrency,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Bill_Period_Ref",STRING,"4","",billPeriodRef,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Source_Account_No",STRING,"10","",sourceAccountNo,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Source_Invoice_No",STRING,"10","",sourceInvoiceNo,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Billed_Date",DATE,"","","","",billedDate);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Curr_Period_Start_Date",DATE,"","","","",currPeriodStartDate);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Curr_Period_End_Date",DATE,"","","","",currPeriodEndDate);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Total_Usage_Charges",NUMBER,"10","2","",totalUsageCharges,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
      }
    }
    catch(java.sql.SQLException ex)
    {
      message = "   Error getting switched VPN report details for conglom invoice "+invoiceNo+" : "+ex.toString();
      System.out.println(message);
      writeToLogFile(message);
      result = false;
    }
    finally
    {
      reportRS=null;
    }
    return result;
  }


  private boolean conglomSwitchedSundry(String globalCustomerId, String accountName, String invoiceNo)
  {
    boolean result = true;
    String rName = "cb_ssvo_sundry";
    // supplementary line
    String workLine =
      supplementaryLine(
        bsName,
        workFileNo,
        recordSeqNoReports,
        globalCustomerId,
        accountName,
        invoiceNo,
        CHARGE,
        billDate,
        "0.00",
        "0.00",
        invoiceCurrency,
        invoiceCurrency,
        rName,
        "1.00",
        "",
        CONGLOM);
    writeToWork2File(workLine);
    recordSeqNoReports++;
    ResultSet reportRS = eDB.getReportDetails(invoiceNo,rName);
    try
    {
          while (reportRS.next())
      {
        String rName2 = tidyString(reportRS.getString("R_Name"));
        String conglomCustName = tidyString(reportRS.getString("Conglom_Cust_Name"));
        String cwContactTitle = tidyString(reportRS.getString("CW_Contact_Title"));
        String cwContactName = tidyString(reportRS.getString("CW_Contact_Name"));
        String conglomInvoiceRef = tidyString(reportRS.getString("Conglom_Invoice_Ref"));
        String billDate = reformatConglomDate(reportRS.getString("Bill_Date"));
        String invoiceCurrency = tidyString(reportRS.getString("Invoice_Currency"));
        String billPeriodRef = tidyString(reportRS.getString("Bill_Period_Ref"));
        String sourceAccountNo = tidyString(reportRS.getString("Source_Account_No"));
        String sourceInvoiceNo = tidyString(reportRS.getString("Source_Invoice_No"));
        String billedDate = reformatConglomDate(reportRS.getString("Billed_Date"));
        String description = tidyString(reportRS.getString("Description"));
        String netAmount = tidyString(reportRS.getString("Net_Amount"));
        // supplementary detail
        workLine =
          supplementaryDetail(
            bsName,
            workFileNo,
            recordSeqNoReports,
            globalCustomerId,
            accountName,
            invoiceNo,
            CHARGE,
            billDate,
            "0.00",
            "0.00",
            invoiceCurrency,
            invoiceCurrency,
            rName,
            "1.00",
            "",
            CONGLOM);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        // source data
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "R_Name",STRING,"50","",rName2,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Conglom_Cust_Name",STRING,"50","",conglomCustName,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "CW_Contact_Title",STRING,"31","",cwContactTitle,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "CW_Contact_Name",STRING,"30","",cwContactName,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Conglom_Invoice_Ref",STRING,"13","",conglomInvoiceRef,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Bill_Date",DATE,"","","","",billDate);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Invoice_Currency",STRING,"3","",invoiceCurrency,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Bill_Period_Ref",STRING,"4","",billPeriodRef,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Source_Account_No",STRING,"10","",sourceAccountNo,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Source_Invoice_No",STRING,"10","",sourceInvoiceNo,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Billed_Date",DATE,"","","","",billedDate);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Description",STRING,"50","",description,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Net_Amount",NUMBER,"10","2","",netAmount,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
      }
    }
    catch(java.sql.SQLException ex)
    {
      message = "   Error getting switched sundry report details for conglom invoice "+invoiceNo+" : "+ex.toString();
      System.out.println(message);
      writeToLogFile(message);
      result = false;
    }
    finally
    {
      reportRS=null;
    }
    return result;
  }

  private boolean conglomSwitchedInstallCharges(String globalCustomerId, String accountName, String invoiceNo)
  {
    boolean result = true;
    String rName = "cb_ssvo_install_charges";
    // supplementary line
    String workLine =
      supplementaryLine(
        bsName,
        workFileNo,
        recordSeqNoReports,
        globalCustomerId,
        accountName,
        invoiceNo,
        CHARGE,
        billDate,
        "0.00",
        "0.00",
        invoiceCurrency,
        invoiceCurrency,
        rName,
        "1.00",
        "",
        CONGLOM);
    writeToWork2File(workLine);
    recordSeqNoReports++;
    ResultSet reportRS = eDB.getReportDetails(invoiceNo,rName);
    try
    {
          while (reportRS.next())
      {
        String conglomCustName = tidyString(reportRS.getString("Conglom_Cust_Name"));
        String cwContactTitle = tidyString(reportRS.getString("CW_Contact_Title"));
        String cwContactName = tidyString(reportRS.getString("CW_Contact_Name"));
        String conglomInvoiceRef = tidyString(reportRS.getString("Conglom_Invoice_Ref"));
        String billDate = reformatConglomDate(reportRS.getString("Bill_Date"));
        String invoiceCurrency = tidyString(reportRS.getString("Invoice_Currency"));
        String billPeriodRef = tidyString(reportRS.getString("Bill_Period_Ref"));
        String sourceAccountNo = tidyString(reportRS.getString("Source_Account_No"));
        String sourceInvoiceNo = tidyString(reportRS.getString("Source_Invoice_No"));
        String billedDate = reformatConglomDate(reportRS.getString("Billed_Date"));
        String chargeDate = reformatConglomDate(reportRS.getString("Charge_Date"));
        String chargeDescription = tidyString(reportRS.getString("Charge_Description"));
        String chargeQuantity = tidyString(reportRS.getString("Charge_Quantity"));
        String chargeAmount = tidyString(reportRS.getString("Charge_Amount"));
        // supplementary detail
        workLine =
          supplementaryDetail(
            bsName,
            workFileNo,
            recordSeqNoReports,
            globalCustomerId,
            accountName,
            invoiceNo,
            CHARGE,
            billDate,
            "0.00",
            "0.00",
            invoiceCurrency,
            invoiceCurrency,
            rName,
            "1.00",
            "",
            CONGLOM);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        // source data
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Conglom_Cust_Name",STRING,"50","",conglomCustName,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "CW_Contact_Title",STRING,"31","",cwContactTitle,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "CW_Contact_Name",STRING,"30","",cwContactName,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Conglom_Invoice_Ref",STRING,"13","",conglomInvoiceRef,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Bill_Date",DATE,"","","","",billDate);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Invoice_Currency",STRING,"3","",invoiceCurrency,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Bill_Period_Ref",STRING,"4","",billPeriodRef,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Source_Account_No",STRING,"10","",sourceAccountNo,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Source_Invoice_No",STRING,"10","",sourceInvoiceNo,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Billed_Date",DATE,"","","","",billedDate);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Charge_Date",DATE,"","","","",chargeDate);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Charge_Description",STRING,"50","",chargeDescription,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Charge_Quantity",NUMBER,"10","0","",chargeQuantity,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Charge_Amount",NUMBER,"10","2","",chargeAmount,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
      }
    }
    catch(java.sql.SQLException ex)
    {
      message = "   Error getting switched install charge report details for conglom invoice "+invoiceNo+" : "+ex.toString();
      System.out.println(message);
      writeToLogFile(message);
      result = false;
    }
    finally
    {
      reportRS=null;
    }
    return result;
  }

  private boolean conglomSwitchedEasyUsageCharges(String globalCustomerId, String accountName, String invoiceNo)
  {
    boolean result = true;
    String rName = "cb_ssvo_easy_usage_charges";
    // supplementary line
    String workLine =
      supplementaryLine(
        bsName,
        workFileNo,
        recordSeqNoReports,
        globalCustomerId,
        accountName,
        invoiceNo,
        CHARGE,
        billDate,
        "0.00",
        "0.00",
        invoiceCurrency,
        invoiceCurrency,
        rName,
        "1.00",
        "",
        CONGLOM);
    writeToWork2File(workLine);
    recordSeqNoReports++;
    ResultSet reportRS = eDB.getReportDetails(invoiceNo,rName);
    try
    {
          while (reportRS.next())
      {
        String conglomCustName = tidyString(reportRS.getString("Conglom_Cust_Name"));
        String cwContactTitle = tidyString(reportRS.getString("CW_Contact_Title"));
        String cwContactName = tidyString(reportRS.getString("CW_Contact_Name"));
        String conglomInvoiceRef = tidyString(reportRS.getString("Conglom_Invoice_Ref"));
        String billDate = reformatConglomDate(reportRS.getString("Bill_Date"));
        String invoiceCurrency = tidyString(reportRS.getString("Invoice_Currency"));
        String billPeriodRef = tidyString(reportRS.getString("Bill_Period_Ref"));
        String sourceAccountNo = tidyString(reportRS.getString("Source_Account_No"));
        String sourceInvoiceNo = tidyString(reportRS.getString("Source_Invoice_No"));
        String billedDate = reformatConglomDate(reportRS.getString("Billed_Date"));
        String currPeriodStartDate = reformatConglomDate(reportRS.getString("Curr_Period_Start_Date"));
        String currPeriodEndDate = reformatConglomDate(reportRS.getString("Curr_Period_End_Date"));
        String currPeriodUsageTotal = tidyString(reportRS.getString("Curr_Period_Usage_Total"));
        String prevPeriodStartDate = reformatConglomDate(reportRS.getString("Prev_Period_Start_Date"));
        String prevPeriodEndDate = reformatConglomDate(reportRS.getString("Prev_Period_End_Date"));
        String prevPeriodUsageTotal = tidyString(reportRS.getString("Prev_Period_Usage_Total"));
        String totalUsageCharges = tidyString(reportRS.getString("Total_Usage_Charges"));
        // supplementary detail
        workLine =
          supplementaryDetail(
            bsName,
            workFileNo,
            recordSeqNoReports,
            globalCustomerId,
            accountName,
            invoiceNo,
            CHARGE,
            billDate,
            "0.00",
            "0.00",
            invoiceCurrency,
            invoiceCurrency,
            rName,
            "1.00",
            "",
            CONGLOM);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        // source data
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Conglom_Cust_Name",STRING,"50","",conglomCustName,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "CW_Contact_Title",STRING,"31","",cwContactTitle,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "CW_Contact_Name",STRING,"30","",cwContactName,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Conglom_Invoice_Ref",STRING,"13","",conglomInvoiceRef,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Bill_Date",DATE,"","","","",billDate);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Invoice_Currency",STRING,"3","",invoiceCurrency,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Bill_Period_Ref",STRING,"4","",billPeriodRef,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Source_Account_No",STRING,"10","",sourceAccountNo,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Source_Invoice_No",STRING,"10","",sourceInvoiceNo,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Billed_Date",DATE,"","","","",billedDate);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Curr_Period_Start_Date",DATE,"","","","",currPeriodStartDate);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Curr_Period_End_Date",DATE,"","","","",currPeriodEndDate);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Curr_Period_Usage_Total",NUMBER,"10","2","",currPeriodUsageTotal,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Prev_Period_Start_Date",DATE,"","","","",prevPeriodStartDate);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Prev_Period_End_Date",DATE,"","","","",prevPeriodEndDate);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Prev_Period_Usage_Total",NUMBER,"10","2","",prevPeriodUsageTotal,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Total_Usage_Charges",NUMBER,"10","2","",totalUsageCharges,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
      }
    }
    catch(java.sql.SQLException ex)
    {
      message = "   Error getting switched easy access usage charges report details for conglom invoice "+invoiceNo+" : "+ex.toString();
      System.out.println(message);
      writeToLogFile(message);
      result = false;
    }
    finally
    {
      reportRS=null;
    }
    return result;
  }

  private boolean conglomSwitchedAdjustments(String globalCustomerId, String accountName, String invoiceNo)
  {
    boolean result = true;
    String rName = "cb_ssvo_adjustments";
    // supplementary line
    String workLine =
      supplementaryLine(
        bsName,
        workFileNo,
        recordSeqNoReports,
        globalCustomerId,
        accountName,
        invoiceNo,
        CHARGE,
        billDate,
        "0.00",
        "0.00",
        invoiceCurrency,
        invoiceCurrency,
        rName,
        "1.00",
        "",
        CONGLOM);
    writeToWork2File(workLine);
    recordSeqNoReports++;
    ResultSet reportRS = eDB.getReportDetails(invoiceNo,rName);
    try
    {
          while (reportRS.next())
      {
        String conglomCustName = tidyString(reportRS.getString("Conglom_Cust_Name"));
        String cwContactTitle = tidyString(reportRS.getString("CW_Contact_Title"));
        String cwContactName = tidyString(reportRS.getString("CW_Contact_Name"));
        String conglomInvoiceRef = tidyString(reportRS.getString("Conglom_Invoice_Ref"));
        String billDate = reformatConglomDate(reportRS.getString("Bill_Date"));
        String invoiceCurrency = tidyString(reportRS.getString("Invoice_Currency"));
        String billPeriodRef = tidyString(reportRS.getString("Bill_Period_Ref"));
        String sourceAccountNo = tidyString(reportRS.getString("Source_Account_No"));
        String sourceInvoiceNo = tidyString(reportRS.getString("Source_Invoice_No"));
        String billedDate = reformatConglomDate(reportRS.getString("Billed_Date"));
        String adjustmentType = tidyString(reportRS.getString("Adjustment_Type"));
        String dateRaised = reformatConglomDate(reportRS.getString("Date_Raised"));
        String docketNumber = tidyString(reportRS.getString("Docket_Number"));
        String description = tidyString(reportRS.getString("Description"));
        String creditAmount = tidyString(reportRS.getString("Credit_Amount"));
        String adjAmount = tidyString(reportRS.getString("Adj_Amount"));
        // supplementary detail
        workLine =
          supplementaryDetail(
            bsName,
            workFileNo,
            recordSeqNoReports,
            globalCustomerId,
            accountName,
            invoiceNo,
            CHARGE,
            billDate,
            "0.00",
            "0.00",
            invoiceCurrency,
            invoiceCurrency,
            rName,
            "1.00",
            "",
            CONGLOM);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        // source data
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Conglom_Cust_Name",STRING,"50","",conglomCustName,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "CW_Contact_Title",STRING,"31","",cwContactTitle,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "CW_Contact_Name",STRING,"30","",cwContactName,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Conglom_Invoice_Ref",STRING,"13","",conglomInvoiceRef,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Bill_Date",DATE,"","","","",billDate);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Invoice_Currency",STRING,"3","",invoiceCurrency,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Bill_Period_Ref",STRING,"4","",billPeriodRef,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Source_Account_No",STRING,"10","",sourceAccountNo,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Source_Invoice_No",STRING,"10","",sourceInvoiceNo,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Billed_Date",DATE,"","","","",billedDate);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Adjustment_Type",STRING,"4","",adjustmentType,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Date_Raised",DATE,"","","","",dateRaised);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Docket_Number",STRING,"10","",docketNumber,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Description",STRING,"10","",description,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Credit_Amount",NUMBER,"10","2","",creditAmount,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Adj_Amount",NUMBER,"10","2","",adjAmount,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
      }
    }
    catch(java.sql.SQLException ex)
    {
      message = "   Error getting switched adjustments report details for conglom invoice "+invoiceNo+" : "+ex.toString();
      System.out.println(message);
      writeToLogFile(message);
      result = false;
    }
    finally
    {
      reportRS=null;
    }
    return result;
  }

  private boolean conglomSwitchedCallink(String globalCustomerId, String accountName, String invoiceNo)
  {
    boolean result = true;
    String rName = "cb_ssvo_callink";
    // supplementary line
    String workLine =
      supplementaryLine(
        bsName,
        workFileNo,
        recordSeqNoReports,
        globalCustomerId,
        accountName,
        invoiceNo,
        CHARGE,
        billDate,
        "0.00",
        "0.00",
        invoiceCurrency,
        invoiceCurrency,
        rName,
        "1.00",
        "",
        CONGLOM);
    writeToWork2File(workLine);
    recordSeqNoReports++;
    ResultSet reportRS = eDB.getReportDetails(invoiceNo,rName);
    try
    {
          while (reportRS.next())
      {
        String conglomCustName = tidyString(reportRS.getString("Conglom_Cust_Name"));
        String cwContactTitle = tidyString(reportRS.getString("CW_Contact_Title"));
        String cwContactName = tidyString(reportRS.getString("CW_Contact_Name"));
        String conglomInvoiceRef = tidyString(reportRS.getString("Conglom_Invoice_Ref"));
        String billDate = reformatConglomDate(reportRS.getString("Bill_Date"));
        String invoiceCurrency = tidyString(reportRS.getString("Invoice_Currency"));
        String billPeriodRef = tidyString(reportRS.getString("Bill_Period_Ref"));
        String sourceAccountNo = tidyString(reportRS.getString("Source_Account_No"));
        String sourceInvoiceNo = tidyString(reportRS.getString("Source_Invoice_No"));
        String billedDate = reformatConglomDate(reportRS.getString("Billed_Date"));
        String chargeStartDate = reformatConglomDate(reportRS.getString("Charge_Start_Date"));
        String chargeEndDate = reformatConglomDate(reportRS.getString("Charge_End_Date"));
        String chargeId = tidyString(reportRS.getString("Charge_Id"));
        String chargeDescription = tidyString(reportRS.getString("Charge_Description"));
        String chargeAmount = tidyString(reportRS.getString("Charge_Amount"));
        // supplementary detail
        workLine =
          supplementaryDetail(
            bsName,
            workFileNo,
            recordSeqNoReports,
            globalCustomerId,
            accountName,
            invoiceNo,
            CHARGE,
            billDate,
            "0.00",
            "0.00",
            invoiceCurrency,
            invoiceCurrency,
            rName,
            "1.00",
            "",
            CONGLOM);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        // source data
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Conglom_Cust_Name",STRING,"50","",conglomCustName,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "CW_Contact_Title",STRING,"31","",cwContactTitle,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "CW_Contact_Name",STRING,"30","",cwContactName,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Conglom_Invoice_Ref",STRING,"13","",conglomInvoiceRef,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Bill_Date",DATE,"","","","",billDate);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Invoice_Currency",STRING,"3","",invoiceCurrency,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Bill_Period_Ref",STRING,"4","",billPeriodRef,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Source_Account_No",STRING,"10","",sourceAccountNo,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Source_Invoice_No",STRING,"10","",sourceInvoiceNo,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Billed_Date",DATE,"","","","",billedDate);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Charge_Start_Date",DATE,"","","","",chargeStartDate);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Charge_End_Date",DATE,"","","","",chargeEndDate);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Charge_Id",NUMBER,"6","0","",chargeId,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Charge_Description",STRING,"250","",chargeDescription,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Charge_Amount",NUMBER,"10","2","",chargeAmount,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
      }
    }
    catch(java.sql.SQLException ex)
    {
      message = "   Error getting switched callink report details for conglom invoice "+invoiceNo+" : "+ex.toString();
      System.out.println(message);
      writeToLogFile(message);
      result = false;
    }
    finally
    {
      reportRS=null;
    }
    return result;
  }

  private boolean conglomSwitchedSourceDiscount(String globalCustomerId, String accountName, String invoiceNo)
  {
    boolean result = true;
    String rName = "cb_ssvo_source_discount";
    // supplementary line
    String workLine =
      supplementaryLine(
        bsName,
        workFileNo,
        recordSeqNoReports,
        globalCustomerId,
        accountName,
        invoiceNo,
        CHARGE,
        billDate,
        "0.00",
        "0.00",
        invoiceCurrency,
        invoiceCurrency,
        rName,
        "1.00",
        "",
        CONGLOM);
    writeToWork2File(workLine);
    recordSeqNoReports++;
    ResultSet reportRS = eDB.getReportDetails(invoiceNo,rName);
    try
    {
          while (reportRS.next())
      {
        String conglomCustName = tidyString(reportRS.getString("Conglom_Cust_Name"));
        String cwContactTitle = tidyString(reportRS.getString("CW_Contact_Title"));
        String cwContactName = tidyString(reportRS.getString("CW_Contact_Name"));
        String conglomInvoiceRef = tidyString(reportRS.getString("Conglom_Invoice_Ref"));
        String billDate = reformatConglomDate(reportRS.getString("Bill_Date"));
        String invoiceCurrency = tidyString(reportRS.getString("Invoice_Currency"));
        String sourceAccountNo = tidyString(reportRS.getString("Source_Account_No"));
        String sourceInvoiceNo = tidyString(reportRS.getString("Source_Invoice_No"));
        String billedDate = reformatConglomDate(reportRS.getString("Billed_Date"));
        String discountCategory = tidyString(reportRS.getString("Discount_Category"));
        String discountDescription = tidyString(reportRS.getString("Discount_Description"));
        String discountAmount = tidyString(reportRS.getString("Discount_Amount"));
        // supplementary detail
        workLine =
          supplementaryDetail(
            bsName,
            workFileNo,
            recordSeqNoReports,
            globalCustomerId,
            accountName,
            invoiceNo,
            CHARGE,
            billDate,
            "0.00",
            "0.00",
            invoiceCurrency,
            invoiceCurrency,
            rName,
            "1.00",
            "",
            CONGLOM);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        // source data
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Conglom_Cust_Name",STRING,"50","",conglomCustName,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "CW_Contact_Title",STRING,"31","",cwContactTitle,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "CW_Contact_Name",STRING,"30","",cwContactName,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Conglom_Invoice_Ref",STRING,"13","",conglomInvoiceRef,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Bill_Date",DATE,"","","","",billDate);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Invoice_Currency",STRING,"3","",invoiceCurrency,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Source_Account_No",STRING,"10","",sourceAccountNo,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Source_Invoice_No",STRING,"10","",sourceInvoiceNo,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Billed_Date",DATE,"","","","",billedDate);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Discount_Category",STRING,"50","",discountCategory,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Discount_Description",STRING,"50","",discountDescription,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Discount_Amount",NUMBER,"10","2","",discountAmount,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
      }
    }
    catch(java.sql.SQLException ex)
    {
      message = "   Error getting switched source discount report details for conglom invoice "+invoiceNo+" : "+ex.toString();
      System.out.println(message);
      writeToLogFile(message);
      result = false;
    }
    finally
    {
      reportRS=null;
    }
    return result;
  }

  private boolean conglomSwitchedRentals(String globalCustomerId, String accountName, String invoiceNo)
  {
    boolean result = true;
    String rName = "cb_ssvo_rentals";
    // supplementary line
    String workLine =
      supplementaryLine(
        bsName,
        workFileNo,
        recordSeqNoReports,
        globalCustomerId,
        accountName,
        invoiceNo,
        CHARGE,
        billDate,
        "0.00",
        "0.00",
        invoiceCurrency,
        invoiceCurrency,
        rName,
        "1.00",
        "",
        CONGLOM);
    writeToWork2File(workLine);
    recordSeqNoReports++;
    ResultSet reportRS = eDB.getReportDetails(invoiceNo,rName);
    try
    {
          while (reportRS.next())
      {
        String conglomCustName = tidyString(reportRS.getString("Conglom_Cust_Name"));
        String cwContactTitle = tidyString(reportRS.getString("CW_Contact_Title"));
        String cwContactName = tidyString(reportRS.getString("CW_Contact_Name"));
        String conglomInvoiceRef = tidyString(reportRS.getString("Conglom_Invoice_Ref"));
        String billDate = reformatConglomDate(reportRS.getString("Bill_Date"));
        String invoiceCurrency = tidyString(reportRS.getString("Invoice_Currency"));
        String billPeriodRef = tidyString(reportRS.getString("Bill_Period_Ref"));
        String sourceAccountNo = tidyString(reportRS.getString("Source_Account_No"));
        String sourceInvoiceNo = tidyString(reportRS.getString("Source_Invoice_No"));
        String billedDate = reformatConglomDate(reportRS.getString("Billed_Date"));
        String periodStartDate = reformatConglomDate(reportRS.getString("Period_Start_Date"));
        String periodEndDate = reformatConglomDate(reportRS.getString("Period_End_Date"));
        String circuitDescription = tidyString(reportRS.getString("Circuit_Description"));
        String quantity = tidyString(reportRS.getString("Quantity"));
        String netAmount = tidyString(reportRS.getString("Net_Amount"));
        // supplementary detail
        workLine =
          supplementaryDetail(
            bsName,
            workFileNo,
            recordSeqNoReports,
            globalCustomerId,
            accountName,
            invoiceNo,
            CHARGE,
            billDate,
            "0.00",
            "0.00",
            invoiceCurrency,
            invoiceCurrency,
            rName,
            "1.00",
            "",
            CONGLOM);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        // source data
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Conglom_Cust_Name",STRING,"50","",conglomCustName,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "CW_Contact_Title",STRING,"31","",cwContactTitle,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "CW_Contact_Name",STRING,"30","",cwContactName,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Conglom_Invoice_Ref",STRING,"13","",conglomInvoiceRef,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Bill_Date",DATE,"","","","",billDate);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Invoice_Currency",STRING,"3","",invoiceCurrency,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Bill_Period_Ref",STRING,"4","",billPeriodRef,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Source_Account_No",STRING,"10","",sourceAccountNo,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Source_Invoice_No",STRING,"10","",sourceInvoiceNo,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Billed_Date",DATE,"","","","",billedDate);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Period_Start_Date",DATE,"","","","",periodStartDate);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Period_End_Date",DATE,"","","","",periodEndDate);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Circuit_Description",STRING,"10","",circuitDescription,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Quantity",NUMBER,"10","0","",quantity,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Net_Amount",NUMBER,"10","2","",netAmount,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
      }
    }
    catch(java.sql.SQLException ex)
    {
      message = "   Error getting switched rentals report details for conglom invoice "+invoiceNo+" : "+ex.toString();
      System.out.println(message);
      writeToLogFile(message);
      result = false;
    }
    finally
    {
      reportRS=null;
    }
    return result;
  }

  private boolean conglomSwitchedCallChargesSummary(String globalCustomerId, String accountName, String invoiceNo)
  {
    boolean result = true;
    String rName = "cb_ssvo_call_charges_summary";
    // supplementary line
    String workLine =
      supplementaryLine(
        bsName,
        workFileNo,
        recordSeqNoReports,
        globalCustomerId,
        accountName,
        invoiceNo,
        CHARGE,
        billDate,
        "0.00",
        "0.00",
        invoiceCurrency,
        invoiceCurrency,
        rName,
        "1.00",
        "",
        CONGLOM);
    writeToWork2File(workLine);
    recordSeqNoReports++;
    ResultSet reportRS = eDB.getReportDetails(invoiceNo,rName);
    try
    {
          while (reportRS.next())
      {
        String conglomCustName = tidyString(reportRS.getString("Conglom_Cust_Name"));
        String cwContactTitle = tidyString(reportRS.getString("CW_Contact_Title"));
        String cwContactName = tidyString(reportRS.getString("CW_Contact_Name"));
        String conglomInvoiceRef = tidyString(reportRS.getString("Conglom_Invoice_Ref"));
        String billDate = reformatConglomDate(reportRS.getString("Bill_Date"));
        String invoiceCurrency = tidyString(reportRS.getString("Invoice_Currency"));
        String billPeriodRef = tidyString(reportRS.getString("Bill_Period_Ref"));
        String sourceAccountNo = tidyString(reportRS.getString("Source_Account_No"));
        String sourceInvoiceNo = tidyString(reportRS.getString("Source_Invoice_No"));
        String billedDate = reformatConglomDate(reportRS.getString("Billed_Date"));
        String currPeriodStartDate = reformatConglomDate(reportRS.getString("Curr_Period_Start_Date"));
        String currPeriodEndDate = reformatConglomDate(reportRS.getString("Curr_Period_End_Date"));
        String currPeriodUsageTotal = tidyString(reportRS.getString("Curr_Period_Usage_Total"));
        String prevPeriodStartDate = reformatConglomDate(reportRS.getString("Prev_Period_Start_Date"));
        String prevPeriodEndDate = reformatConglomDate(reportRS.getString("Prev_Period_End_Date"));
        String prevPeriodUsageTotal = tidyString(reportRS.getString("Prev_Period_Usage_Total"));
        String totalUsageCharges = tidyString(reportRS.getString("Total_Usage_Charges"));
        // supplementary detail
        workLine =
          supplementaryDetail(
            bsName,
            workFileNo,
            recordSeqNoReports,
            globalCustomerId,
            accountName,
            invoiceNo,
            CHARGE,
            billDate,
            "0.00",
            "0.00",
            invoiceCurrency,
            invoiceCurrency,
            rName,
            "1.00",
            "",
            CONGLOM);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        // source data
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Conglom_Cust_Name",STRING,"50","",conglomCustName,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "CW_Contact_Title",STRING,"31","",cwContactTitle,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "CW_Contact_Name",STRING,"30","",cwContactName,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Conglom_Invoice_Ref",STRING,"13","",conglomInvoiceRef,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Bill_Date",DATE,"","","","",billDate);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Invoice_Currency",STRING,"3","",invoiceCurrency,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Bill_Period_Ref",STRING,"4","",billPeriodRef,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Source_Account_No",STRING,"10","",sourceAccountNo,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Source_Invoice_No",STRING,"10","",sourceInvoiceNo,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Billed_Date",DATE,"","","","",billedDate);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Curr_Period_Start_Date",DATE,"","","","",currPeriodStartDate);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Curr_Period_End_Date",DATE,"","","","",currPeriodEndDate);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Curr_Period_Usage_Total",NUMBER,"10","2","",currPeriodUsageTotal,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Prev_Period_Start_Date",DATE,"","","","",prevPeriodStartDate);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Prev_Period_End_Date",DATE,"","","","",prevPeriodEndDate);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Prev_Period_Usage_Total",NUMBER,"10","2","",prevPeriodUsageTotal,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Total_Usage_Charges",NUMBER,"10","2","",totalUsageCharges,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
      }
    }
    catch(java.sql.SQLException ex)
    {
      message = "   Error getting switched call charges summary report details for conglom invoice "+invoiceNo+" : "+ex.toString();
      System.out.println(message);
      writeToLogFile(message);
      result = false;
    }
    finally
    {
      reportRS=null;
    }
    return result;
  }

  private boolean conglomSwitchedConsolLD(String globalCustomerId, String accountName, String invoiceNo)
  {
    boolean result = true;
    String rName = "cb_ssvo_consolidated";
    // supplementary line
    String workLine =
      supplementaryLine(
        bsName,
        workFileNo,
        recordSeqNoReports,
        globalCustomerId,
        accountName,
        invoiceNo,
        CHARGE,
        billDate,
        "0.00",
        "0.00",
        invoiceCurrency,
        invoiceCurrency,
        rName,
        "1.00",
        "",
        CONGLOM);
    writeToWork2File(workLine);
    recordSeqNoReports++;
    if (eDB.getLDCount(invoiceNo,CONSOL,SSVO)>0)
    {
      ResultSet reportRS = eDB.getReportDetails(invoiceNo,rName);
      try
      {
        while (reportRS.next())
        {
          String subTotalLevel1 = tidyString(reportRS.getString("Sub_Total_Level1"));
          String subTotalLevel2 = tidyString(reportRS.getString("Sub_Total_Level2"));
          String subTField1 = tidyString(reportRS.getString("SubT_Field_1"));
          String subTField2 = tidyString(reportRS.getString("SubT_Field_2"));
          String conglomCustName = tidyString(reportRS.getString("Conglom_Cust_Name"));
          String cwContactTitle = tidyString(reportRS.getString("CW_Contact_Title"));
          String cwContactName = tidyString(reportRS.getString("CW_Contact_Name"));
          String conglomInvoiceRef = tidyString(reportRS.getString("Conglom_Invoice_Ref"));
          String billDate = reformatConglomDate(reportRS.getString("Bill_Date"));
          String invoiceCurrency = tidyString(reportRS.getString("Invoice_Currency"));
          String billPeriodRef = tidyString(reportRS.getString("Bill_Period_Ref"));
          String sourceAccountNo = tidyString(reportRS.getString("Source_Account_No"));
          String sourceInvoiceNo = tidyString(reportRS.getString("Source_Invoice_No"));
          String ldHeader1 = reportRS.getString("LD_Header1");
          String ldHeader2 = reportRS.getString("LD_Header2");
          String ldHeader3 = reportRS.getString("LD_Header3");
          String reportLDString = tidyString2(reportRS.getString("Report_LD_String"));
          String billedDate = reformatConglomDate(reportRS.getString("Billed_Date"));
          String oneOffCharges = tidyString(reportRS.getString("One_Off_Charges"));
          String recurringCharges = tidyString(reportRS.getString("Recurring_Charges"));
          String usageCharges = tidyString(reportRS.getString("Usage_Charges"));
          String miscCharges = tidyString(reportRS.getString("Misc_Charges"));
          String adjustmentTotal = tidyString(reportRS.getString("Adjustment_Total"));
          String sourceDiscTotal = tidyString(reportRS.getString("Source_Disc_Total"));
          String conglomDiscTotal = tidyString(reportRS.getString("Conglom_Disc_Total"));
          String acctBalNetAmount = tidyString(reportRS.getString("Acct_Bal_Net_Amount"));
          String acctBalVatAmount = tidyString(reportRS.getString("Acct_Bal_Vat_Amount"));
          String acctBalAmount = tidyString(reportRS.getString("Acct_Bal_Amount"));
          String ldSortItem1 = tidyString(reportRS.getString("LD_Sort_Item1"));
          String ldSortItem2 = tidyString(reportRS.getString("LD_Sort_Item2"));
          String ldSortItem3 = tidyString(reportRS.getString("LD_Sort_Item3"));
          // supplementary detail
          workLine =
            supplementaryDetail(
              bsName,
              workFileNo,
              recordSeqNoReports,
              globalCustomerId,
              accountName,
              invoiceNo,
              CHARGE,
              billDate,
              "0.00",
              "0.00",
              invoiceCurrency,
              invoiceCurrency,
              rName,
              "1.00",
              "",
              CONGLOM);
          writeToWork2File(workLine);
          recordSeqNoReports++;
          // source data
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Sub_Total_Level1",STRING,"3","",subTotalLevel1,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Sub_Total_Level2",STRING,"3","",subTotalLevel2,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "SubT_Field_1",STRING,"3","",subTField1,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "SubT_Field_2",STRING,"3","",subTField2,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Conglom_Cust_Name",STRING,"50","",conglomCustName,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "CW_Contact_Title",STRING,"31","",cwContactTitle,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "CW_Contact_Name",STRING,"30","",cwContactName,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Conglom_Invoice_Ref",STRING,"13","",conglomInvoiceRef,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Bill_Date",DATE,"","","","",billDate);
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Invoice_Currency",STRING,"3","",invoiceCurrency,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Bill_Period_Ref",STRING,"4","",billPeriodRef,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Source_Account_No",STRING,"10","",sourceAccountNo,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Source_Invoice_No",STRING,"10","",sourceInvoiceNo,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "LD_Header1",STRING,"70","",ldHeader1,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "LD_Header2",STRING,"70","",ldHeader2,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "LD_Header3",STRING,"70","",ldHeader3,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Report_LD_String",STRING,"50","",reportLDString,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Billed_Date",DATE,"","","","",billedDate);
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "One_Off_Charges",NUMBER,"10","2","",oneOffCharges,"");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Recurring_Charges",NUMBER,"10","2","",recurringCharges,"");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Usage_Charges",NUMBER,"10","2","",usageCharges,"");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Misc_Charges",NUMBER,"10","2","",miscCharges,"");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Adjustment_Total",NUMBER,"10","2","",adjustmentTotal,"");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Source_Disc_Total",NUMBER,"10","2","",sourceDiscTotal,"");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Conglom_Disc_Total",NUMBER,"10","2","",conglomDiscTotal,"");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Acct_Bal_Net_Amount",NUMBER,"10","2","",acctBalNetAmount,"");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Acct_Bal_Vat_Amount",NUMBER,"10","2","",acctBalVatAmount,"");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "Acct_Bal_Amount",NUMBER,"10","2","",acctBalAmount,"");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "LD_Sort_Item1",STRING,"50","",ldSortItem1,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "LD_Sort_Item2",STRING,"50","",ldSortItem2,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
          workLine =
            sourceData(
              bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
              "LD_Sort_Item3",STRING,"50","",ldSortItem3,"","");
          writeToWork2File(workLine);
          recordSeqNoReports++;
        }
      }
      catch(java.sql.SQLException ex)
      {
        message = "   Error getting switched consolidated with local data report details for conglom invoice "+invoiceNo+" : "+ex.toString();
        System.out.println(message);
        writeToLogFile(message);
        result = false;
      }
      finally
      {
        reportRS=null;
      }
    }
    return result;
  }

  private boolean conglomSwitchedConsolNoLD(String globalCustomerId, String accountName, String invoiceNo)
  {
    boolean result = true;
    String rName = "cb_ssvo_consolidated_nold";
    // supplementary line
    String workLine =
      supplementaryLine(
        bsName,
        workFileNo,
        recordSeqNoReports,
        globalCustomerId,
        accountName,
        invoiceNo,
        CHARGE,
        billDate,
        "0.00",
        "0.00",
        invoiceCurrency,
        invoiceCurrency,
        rName,
        "1.00",
        "",
        CONGLOM);
    writeToWork2File(workLine);
    recordSeqNoReports++;
    ResultSet reportRS = eDB.getReportDetails(invoiceNo,rName);
    try
    {
      while (reportRS.next())
      {
        String subTotalLevel1 = tidyString(reportRS.getString("Sub_Total_Level1"));
        String subTotalLevel2 = tidyString(reportRS.getString("Sub_Total_Level2"));
        String subTField1 = tidyString(reportRS.getString("SubT_Field_1"));
        String subTField2 = tidyString(reportRS.getString("SubT_Field_2"));
        String billedProductId = tidyString(reportRS.getString("Billed_Product_Id"));
        String description = tidyString(reportRS.getString("Description"));
        String conglomCustName = tidyString(reportRS.getString("Conglom_Cust_Name"));
        String cwContactTitle = tidyString(reportRS.getString("CW_Contact_Title"));
        String cwContactName = tidyString(reportRS.getString("CW_Contact_Name"));
        String conglomInvoiceRef = tidyString(reportRS.getString("Conglom_Invoice_Ref"));
        String billDate = reformatConglomDate(reportRS.getString("Bill_Date"));
        String invoiceCurrency = tidyString(reportRS.getString("Invoice_Currency"));
        String billPeriodRef = tidyString(reportRS.getString("Bill_Period_Ref"));
        String sourceAccountNo = tidyString(reportRS.getString("Source_Account_No"));
        String sourceInvoiceNo = tidyString(reportRS.getString("Source_Invoice_No"));
        String billedDate = reformatConglomDate(reportRS.getString("Billed_Date"));
        String oneOffCharges = tidyString(reportRS.getString("One_Off_Charges"));
        String recurringCharges = tidyString(reportRS.getString("Recurring_Charges"));
        String usageCharges = tidyString(reportRS.getString("Usage_Charges"));
        String miscCharges = tidyString(reportRS.getString("Misc_Charges"));
        String adjustmentTotal = tidyString(reportRS.getString("Adjustment_Total"));
        String sourceDiscTotal = tidyString(reportRS.getString("Source_Disc_Total"));
        String conglomDiscTotal = tidyString(reportRS.getString("Conglom_Disc_Total"));
        String acctBalNetAmount = tidyString(reportRS.getString("Acct_Bal_Net_Amount"));
        String acctBalVatAmount = tidyString(reportRS.getString("Acct_Bal_Vat_Amount"));
        String acctBalAmount = tidyString(reportRS.getString("Acct_Bal_Amount"));
        // supplementary detail
        workLine =
          supplementaryDetail(
            bsName,
            workFileNo,
            recordSeqNoReports,
            globalCustomerId,
            accountName,
            invoiceNo,
            CHARGE,
            billDate,
            "0.00",
            "0.00",
            invoiceCurrency,
            invoiceCurrency,
            rName,
            "1.00",
            "",
            CONGLOM);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        // source data
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Sub_Total_Level1",STRING,"3","",subTotalLevel1,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Sub_Total_Level2",STRING,"3","",subTotalLevel2,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "SubT_Field_1",STRING,"3","",subTField1,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "SubT_Field_2",STRING,"3","",subTField2,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Billed_Product_Id",STRING,"4","",billedProductId,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Description",STRING,"50","",description,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Conglom_Cust_Name",STRING,"50","",conglomCustName,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "CW_Contact_Title",STRING,"31","",cwContactTitle,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "CW_Contact_Name",STRING,"30","",cwContactName,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Conglom_Invoice_Ref",STRING,"13","",conglomInvoiceRef,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Bill_Date",DATE,"","","","",billDate);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Invoice_Currency",STRING,"3","",invoiceCurrency,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Bill_Period_Ref",STRING,"4","",billPeriodRef,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Source_Account_No",STRING,"10","",sourceAccountNo,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Source_Invoice_No",STRING,"10","",sourceInvoiceNo,"","");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Billed_Date",DATE,"","","","",billedDate);
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "One_Off_Charges",NUMBER,"10","2","",oneOffCharges,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Recurring_Charges",NUMBER,"10","2","",recurringCharges,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Usage_Charges",NUMBER,"10","2","",usageCharges,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Misc_Charges",NUMBER,"10","2","",miscCharges,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Adjustment_Total",NUMBER,"10","2","",adjustmentTotal,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Source_Disc_Total",NUMBER,"10","2","",sourceDiscTotal,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Conglom_Disc_Total",NUMBER,"10","2","",conglomDiscTotal,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Acct_Bal_Net_Amount",NUMBER,"10","2","",acctBalNetAmount,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Acct_Bal_Vat_Amount",NUMBER,"10","2","",acctBalVatAmount,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNoReports,globalCustomerId,accountName,invoiceNo,
            "Acct_Bal_Amount",NUMBER,"10","2","",acctBalAmount,"");
        writeToWork2File(workLine);
        recordSeqNoReports++;
      }
    }
    catch(java.sql.SQLException ex)
    {
      message = "   Error getting switched consolidated no local data report details for conglom invoice "+invoiceNo+" : "+ex.toString();
      System.out.println(message);
      writeToLogFile(message);
      result = false;
    }
    finally
    {
      reportRS=null;
    }
    return result;
  }

  private boolean conglomInvoiceDetails(String globalCustomerId, String accountName, String invoiceNo)
  {
    boolean result = true;
    String workLine = "";
    ResultSet invoiceDetailsRS = eDB.getConglomInvoiceDetails(invoiceNo);
    try
    {
      if (invoiceDetailsRS.next())
      {
        String invoiceTypeCode = invoiceDetailsRS.getString("Invoice_Type_Code");
        String multiPeriodReportInd = invoiceDetailsRS.getString("Multi_Period_Report_Ind");
        billDate = reformatConglomDate(invoiceDetailsRS.getString("Bill_Date"));
        invoiceCurrency = invoiceDetailsRS.getString("Invoice_Currency");
        String netAmount = invoiceDetailsRS.getString("Net_Amount");
        String vatAmount = invoiceDetailsRS.getString("VAT_Amount");
        String totalAmountDue = invoiceDetailsRS.getString("Total_Amount_Due");
        String bpsd = billDate;
        String bped = billDate;
        long invoiceRecordSeqNo = recordSeqNo;
        recordSeqNo++;
        // charge line
        workLine =
          invoiceLine(
            bsName,
            workFileNo,
            recordSeqNo,
            globalCustomerId,
            accountName,
            invoiceNo,
            CHARGE,
            billDate,
            netAmount,
            netAmount,
            invoiceCurrency,
            invoiceCurrency,
            "Charge Total",
            "1.00",
            "",
            CONGLOM);
        writeToWorkFile(workLine);
        recordSeqNo++;
        // charge line source data
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNo,globalCustomerId,accountName,invoiceNo,
            "Invoice_Type_Code",STRING,"1","",invoiceTypeCode,"","");
        writeToWorkFile(workLine);
        recordSeqNo++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNo,globalCustomerId,accountName,invoiceNo,
            "Multi_Period_Report_Ind",STRING,"1","",multiPeriodReportInd,"","");
        writeToWorkFile(workLine);
        recordSeqNo++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNo,globalCustomerId,accountName,invoiceNo,
            "Qtrly_Billed_Ind",STRING,"1","",qtrlyBilledInd,"","");
        writeToWorkFile(workLine);
        recordSeqNo++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNo,globalCustomerId,accountName,invoiceNo,
            "Qtrly_Billed_Cycle",STRING,"1","",qtrlyBilledCycle,"","");
        writeToWorkFile(workLine);
        recordSeqNo++;
        ResultSet invoiceProductDetailsRS = eDB.getConglomInvoiceProductDetails(invoiceNo);
        try
        {
          while (invoiceProductDetailsRS.next())
          {
            String grp1 = invoiceProductDetailsRS.getString("Grp_1");
            String grp2 = invoiceProductDetailsRS.getString("Grp_2");
            String description = tidyString(invoiceProductDetailsRS.getString("description"));
            String billPeriodRef = invoiceProductDetailsRS.getString("Bill_Period_Ref");
            String oneOffCharges = invoiceProductDetailsRS.getString("One_Off_Charges");
            String recurringCharges = invoiceProductDetailsRS.getString("Recurring_Charges");
            String usageCharges = invoiceProductDetailsRS.getString("Usage_Charges");
            String miscCharges = invoiceProductDetailsRS.getString("Misc_Charges");
            String adjustmentsTotal = invoiceProductDetailsRS.getString("Adjustments_Total");
            String sourceDiscTotal = invoiceProductDetailsRS.getString("Source_Disc_Total");
            String conglomDiscTotal = invoiceProductDetailsRS.getString("Conglom_Disc_Total");
            String netAmountProduct = invoiceProductDetailsRS.getString("Net_Amount");
            String vatAmountProduct = invoiceProductDetailsRS.getString("VAT_Amount");
            String totalAmountDueProduct = invoiceProductDetailsRS.getString("Total_Amount_Due");
            // charge line detail
            workLine =
              invoiceDetail(
                bsName,
                workFileNo,
                recordSeqNo,
                globalCustomerId,
                accountName,
                invoiceNo,
                CHARGE,
                billDate,
                netAmountProduct,
                netAmountProduct,
                invoiceCurrency,
                invoiceCurrency,
                description,
                "1.00",
                "",
                CONGLOM);
            writeToWorkFile(workLine);
            recordSeqNo++;
            // charge detail source data
            workLine =
              sourceData(
                bsName,workFileNo,recordSeqNo,globalCustomerId,accountName,invoiceNo,
                "Description",STRING,"50","",description,"","");
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine =
              sourceData(
                bsName,workFileNo,recordSeqNo,globalCustomerId,accountName,invoiceNo,
                "Grp_1",STRING,"50","",grp1,"","");
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine =
              sourceData(
                bsName,workFileNo,recordSeqNo,globalCustomerId,accountName,invoiceNo,
                "Grp_2",STRING,"50","",grp2,"","");
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine =
              sourceData(
                bsName,workFileNo,recordSeqNo,globalCustomerId,accountName,invoiceNo,
                "Bill_Period_Ref",STRING,"4","",billPeriodRef,"","");
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine =
              sourceData(
                bsName,workFileNo,recordSeqNo,globalCustomerId,accountName,invoiceNo,
                "One_Off_Charges",NUMBER,"10","2","",oneOffCharges,"");
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine =
              sourceData(
                bsName,workFileNo,recordSeqNo,globalCustomerId,accountName,invoiceNo,
                "Recurring_Charges",NUMBER,"10","2","",recurringCharges,"");
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine =
              sourceData(
                bsName,workFileNo,recordSeqNo,globalCustomerId,accountName,invoiceNo,
                "Usage_Charges",NUMBER,"10","2","",usageCharges,"");
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine =
              sourceData(
                bsName,workFileNo,recordSeqNo,globalCustomerId,accountName,invoiceNo,
                "Misc_Charges",NUMBER,"10","2","",miscCharges,"");
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine =
              sourceData(
                bsName,workFileNo,recordSeqNo,globalCustomerId,accountName,invoiceNo,
                "Adjustments_Total",NUMBER,"10","2","",adjustmentsTotal,"");
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine =
              sourceData(
                bsName,workFileNo,recordSeqNo,globalCustomerId,accountName,invoiceNo,
                "Source_Disc_Total",NUMBER,"10","2","",sourceDiscTotal,"");
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine =
              sourceData(
                bsName,workFileNo,recordSeqNo,globalCustomerId,accountName,invoiceNo,
                "Conglom_Disc_Total",NUMBER,"10","2","",conglomDiscTotal,"");
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine =
              sourceData(
                bsName,workFileNo,recordSeqNo,globalCustomerId,accountName,invoiceNo,
                "Vat_Amount",NUMBER,"10","2","",vatAmountProduct,"");
            writeToWorkFile(workLine);
            recordSeqNo++;
            workLine =
              sourceData(
                bsName,workFileNo,recordSeqNo,globalCustomerId,accountName,invoiceNo,
                "Total_Amount_Due",NUMBER,"10","2","",totalAmountDueProduct,"");
            writeToWorkFile(workLine);
            recordSeqNo++;
          }
        }
        catch(java.sql.SQLException ex)
        {
          message = "   Error getting invoice product details for conglom invoice "+invoiceNo+" : "+ex.toString();
          System.out.println(message);
          writeToLogFile(message);
          result = false;
        }
        finally
        {
          invoiceProductDetailsRS=null;
        }
        // tax line
        workLine =
          invoiceLine(
            bsName,
            workFileNo,
            recordSeqNo,
            globalCustomerId,
            accountName,
            invoiceNo,
            TAX,
            billDate,
            vatAmount,
            vatAmount,
            invoiceCurrency,
            invoiceCurrency,
            "Charge Total",
            "1.00",
            vatRate,
            CONGLOM);
        writeToWorkFile(workLine);
        recordSeqNo++;
        // tax line source data
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNo,globalCustomerId,accountName,invoiceNo,
            "Jurisdiction_ExRate",NUMBER,"15","2","","0.00","");
        writeToWorkFile(workLine);
        recordSeqNo++;
        workLine =
          sourceData(
            bsName,workFileNo,recordSeqNo,globalCustomerId,accountName,invoiceNo,
            "Tax_Rate",NUMBER,"2","2","",vatRatePercentage(vatRate),"");
        writeToWorkFile(workLine);
        recordSeqNo++;invoiceProductDetailsRS = eDB.getConglomInvoiceProductDetails(invoiceNo);
        try
        {
          while (invoiceProductDetailsRS.next())
          {
            String grp1 = invoiceProductDetailsRS.getString("Grp_1");
            String grp2 = invoiceProductDetailsRS.getString("Grp_2");
            String description = tidyString(invoiceProductDetailsRS.getString("description"));
            String billPeriodRef = invoiceProductDetailsRS.getString("Bill_Period_Ref");
            String oneOffCharges = invoiceProductDetailsRS.getString("One_Off_Charges");
            String recurringCharges = invoiceProductDetailsRS.getString("Recurring_Charges");
            String usageCharges = invoiceProductDetailsRS.getString("Usage_Charges");
            String miscCharges = invoiceProductDetailsRS.getString("Misc_Charges");
            String adjustmentsTotal = invoiceProductDetailsRS.getString("Adjustments_Total");
            String sourceDiscTotal = invoiceProductDetailsRS.getString("Source_Disc_Total");
            String conglomDiscTotal = invoiceProductDetailsRS.getString("Conglom_Disc_Total");
            String netAmountProduct = invoiceProductDetailsRS.getString("Net_Amount");
            String vatAmountProduct = invoiceProductDetailsRS.getString("VAT_Amount");
            String totalAmountDueProduct = invoiceProductDetailsRS.getString("Total_Amount_Due");
            // tax line detail
            workLine =
              invoiceDetail(
                bsName,
                workFileNo,
                recordSeqNo,
                globalCustomerId,
                accountName,
                invoiceNo,
                CHARGE,
                billDate,
                vatAmountProduct,
                vatAmountProduct,
                invoiceCurrency,
                invoiceCurrency,
                description,
                "1.00",
                vatRate,
                CONGLOM);
            writeToWorkFile(workLine);
            recordSeqNo++;
            // tax detail source data
            workLine =
              sourceData(
                bsName,workFileNo,recordSeqNo,globalCustomerId,accountName,invoiceNo,
                "Tax_Type",STRING,"6","",VAT,"","");
            writeToWorkFile(workLine);
            recordSeqNo++;
          }
        }
        catch(java.sql.SQLException ex)
        {
          message = "   Error getting invoice product details for conglom invoice "+invoiceNo+" : "+ex.toString();
          System.out.println(message);
          writeToLogFile(message);
          result = false;
        }
        finally
        {
          invoiceProductDetailsRS=null;
        }
        ResultSet invoiceBillPeriodsRS = eDB.getConglomInvoiceBillPeriods(invoiceNo);
        try
        {
          if (invoiceBillPeriodsRS.next())
          {
            bpsd = invoiceBillPeriodsRS.getString("BPSD");
            bped = invoiceBillPeriodsRS.getString("BPED");
          }
        }
        catch(java.sql.SQLException ex)
        {
          message = "   Warning - failed to get invoice bill periods for conglom invoice "+invoiceNo+" : "+ex.toString();
          System.out.println(message);
          writeToLogFile(message);
        }
        finally
        {
          invoiceBillPeriodsRS=null;
        }
        workLine =
          invoiceTotalLine(
            bsName,
            workFileNo,
            invoiceRecordSeqNo,
            globalCustomerId,
            accountName,
            invoiceNo,
            billDate,
            billDate,
            bpsd,
            bped,
            totalAmountDue,
            bsName);
        writeToWorkFile(workLine);
      }
      else
      {
        message = "   No invoice details found for conglom invoice "+invoiceNo;
        System.out.println(message);
        writeToLogFile(message);
      }
    }
    catch(java.sql.SQLException ex)
    {
      message = "   Error getting invoice details for conglom invoice "+invoiceNo+" : "+ex.toString();
      System.out.println(message);
      writeToLogFile(message);
      result = false;
    }
    finally
    {
      invoiceDetailsRS = null;
    }
    return result;
    }

  private boolean conglomAccountDetails(String globalCustomerId, String accountName, String invoiceNo)
  {
    boolean result = false;
    String workLine = "";
    ResultSet accountDetailsRS = eDB.getConglomAccountDets(invoiceNo);
    try
    {
      if (accountDetailsRS.next())
      {
        String accountEffectiveFrom = tidyString(accountDetailsRS.getString("Account_Effective_From"));
        String billingCeaseDate = tidyString(accountDetailsRS.getString("Billing_Cease_Date"));
        String glCode = tidyString(accountDetailsRS.getString("GL_Code"));
        String glName = tidyString(accountDetailsRS.getString("GL_Name"));
        String glCustomerNumber = tidyString(accountDetailsRS.getString("GL_Customer_Number"));
        String outgoingCurrencyCode = tidyString(accountDetailsRS.getString("Outgoing_Currency_Code"));
        String cwCompanyName = tidyString(accountDetailsRS.getString("CW_Company_Name"));
        String creditImmediate = tidyString(accountDetailsRS.getString("Credit_Immediate"));
        String billingContact = tidyString(accountDetailsRS.getString("Billing_Contact"));
        String customerContact = tidyString(accountDetailsRS.getString("Customer_Contact"));
        String cwContact = tidyString(accountDetailsRS.getString("CW_Contact"));
        String accountReference = tidyString(accountDetailsRS.getString("Account_Reference"));
        String taxReferenceLiteral = tidyString(accountDetailsRS.getString("Tax_Reference_Literal"));
        String taxReference = tidyString(accountDetailsRS.getString("Tax_Reference"));
        String jurisdictionCurrencyCode = tidyString(accountDetailsRS.getString("Jurisdiction_Currency_Code"));
        String addressEffectiveFrom = tidyString(accountDetailsRS.getString("Address_Effective_From"));
        String billingAddress1 = tidyString(accountDetailsRS.getString("Billing_Address1"));
        String billingAddress2 = tidyString(accountDetailsRS.getString("Billing_Address2"));
        String billingAddress3 = tidyString(accountDetailsRS.getString("Billing_Address3"));
        String billingAddress4 = tidyString(accountDetailsRS.getString("Billing_Address4"));
        String billingAddress5 = tidyString(accountDetailsRS.getString("Billing_Address5"));
        String billingAddress6 = tidyString(accountDetailsRS.getString("Billing_Address6"));
        String billingAddress7 = tidyString(accountDetailsRS.getString("Billing_Address7"));
        String altBillingAddress1 = tidyString(accountDetailsRS.getString("Alt_Billing_Address1"));
        String altBillingAddress2 = tidyString(accountDetailsRS.getString("Alt_Billing_Address2"));
        String altBillingAddress3 = tidyString(accountDetailsRS.getString("Alt_Billing_Address3"));
        String altBillingAddress4 = tidyString(accountDetailsRS.getString("Alt_Billing_Address4"));
        String altBillingAddress5 = tidyString(accountDetailsRS.getString("Alt_Billing_Address5"));
        String altBillingAddress6 = tidyString(accountDetailsRS.getString("Alt_Billing_Address6"));
        String altBillingAddress7 = tidyString(accountDetailsRS.getString("Alt_Billing_Address7"));
        String bankEffectiveFrom = tidyString(accountDetailsRS.getString("Bank_Effective_From"));
        String bankAddress1 = tidyString(accountDetailsRS.getString("Bank_Address1"));
        String bankAddress2 = tidyString(accountDetailsRS.getString("Bank_Address2"));
        String bankAddress3 = tidyString(accountDetailsRS.getString("Bank_Address3"));
        String bankAddress4 = tidyString(accountDetailsRS.getString("Bank_Address4"));
        String bankAddress5 = tidyString(accountDetailsRS.getString("Bank_Address5"));
        String bankAddress6 = tidyString(accountDetailsRS.getString("Bank_Address6"));
        String bankAddress7 = tidyString(accountDetailsRS.getString("Bank_Address7"));
        String altBankAddress1 = tidyString(accountDetailsRS.getString("Alt_Bank_Address1"));
        String altBankAddress2 = tidyString(accountDetailsRS.getString("Alt_Bank_Address2"));
        String altBankAddress3 = tidyString(accountDetailsRS.getString("Alt_Bank_Address3"));
        String altBankAddress4 = tidyString(accountDetailsRS.getString("Alt_Bank_Address4"));
        String altBankAddress5 = tidyString(accountDetailsRS.getString("Alt_Bank_Address5"));
        String altBankAddress6 = tidyString(accountDetailsRS.getString("Alt_Bank_Address6"));
        String altBankAddress7 = tidyString(accountDetailsRS.getString("Alt_Bank_Address7"));
        qtrlyBilledInd = tidyString(accountDetailsRS.getString("Qtrly_Billed_Ind"));
        qtrlyBilledCycle = tidyString(accountDetailsRS.getString("Qtrly_Billed_Cycle"));
        vatRate = tidyString(accountDetailsRS.getString("Vat_Rate"));
        // account record
        workLine =
          accountLine(
            bsName,
            workFileNo,
            recordSeqNo,
            globalCustomerId,
            accountName,
            invoiceNo,
            accountEffectiveFrom,
            glName,
            glCode,
            glCustomerNumber,
            outgoingCurrencyCode,
            cwCompanyName,
            billingContact,
            accountName,
            cwContact,
            customerContact,
            accountReference,
            taxReferenceLiteral,
            taxReference,
            jurisdictionCurrencyCode);
        writeToWorkFile(workLine);
        recordSeqNo++;
        // billing address line
        workLine =
          addressLine(
            bsName,
            workFileNo,
            recordSeqNo,
            globalCustomerId,
            accountName,
            invoiceNo,
            BILLING,
            addressEffectiveFrom,
            billingAddress1,
            billingAddress2,
            billingAddress3,
            billingAddress4,
            billingAddress5,
            billingAddress6,
            billingAddress7,
            "",
            "",
            "");
        writeToWorkFile(workLine);
        recordSeqNo++;
        if (altBillingAddress1.length()>0)
        {
          workLine =
            addressLine(
              bsName,
              workFileNo,
              recordSeqNo,
              globalCustomerId,
              accountName,
              invoiceNo,
              ALTERNATEBILLING,
              addressEffectiveFrom,
              altBillingAddress1,
              altBillingAddress2,
              altBillingAddress3,
              altBillingAddress4,
              altBillingAddress5,
              altBillingAddress6,
              altBillingAddress7,
              "",
              "",
              "");
          writeToWorkFile(workLine);
          recordSeqNo++;
        }
        // bank address line
        workLine =
          bankLine(
            bsName,
            workFileNo,
            recordSeqNo,
            globalCustomerId,
            accountName,
            invoiceNo,
            MAIN,
            bankEffectiveFrom,
            bankAddress1,
            bankAddress2,
            bankAddress3,
            bankAddress4,
            bankAddress5,
            bankAddress6,
            bankAddress7,
            "",
            "",
            "");
          writeToWorkFile(workLine);
          recordSeqNo++;
        // alternate bank address line
        workLine =
          bankLine(
            bsName,
            workFileNo,
            recordSeqNo,
            globalCustomerId,
            accountName,
            invoiceNo,
            ALTERNATE,
            bankEffectiveFrom,
            altBankAddress1,
            altBankAddress2,
            altBankAddress3,
            altBankAddress4,
            altBankAddress5,
            altBankAddress6,
            altBankAddress7,
            "",
            "",
            "");
          writeToWorkFile(workLine);
          recordSeqNo++;

        result = true;
      }
    }
    catch(java.sql.SQLException ex)
    {
      message = "   Error getting account details for conglom invoice "+invoiceNo+" : "+ex.toString();
      System.out.println(message);
      writeToLogFile(message);
    }
    finally
    {
      accountDetailsRS = null;
    }
    return result;
  }

  // convert vat rate to it's percentage value
  private String vatRatePercentage(String vatRate)
  {
    String newValue = vatRate;
    String decPart = "";
    boolean foundPeriod = false;
    for (int s=0; s < vatRate.length(); s++)
    {
      String test = vatRate.substring(s,s+1);
      if (test.startsWith(PERIOD))
        foundPeriod = true;
      else if(foundPeriod)
        decPart = decPart + test;
    }
    newValue = decPart.substring(0,2)+PERIOD+decPart.substring(2,decPart.length());
    return newValue;
  }

}




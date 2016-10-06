package batch;

import java.io.*;
import java.sql.*;
import java.util.*;

public class goldfishExtract
{
  // class variables
  // SQLServer environment variables
  private String sqlServer, sqlServerDb, sqlServerUser, sqlServerPassword;
  private Connection sqlConn, sqlConn2, sqlConn3;
  private sqlServerDB ssDB, ss2DB, ss3DB;
  // Oracle environment variables
  private String oracleServer, oracleDb, oracleUser, oraclePassword, oraclePort;
  private Connection oracleConn, oracleConn2, oracleConn3;
  private oracleDB oDB, o2DB, o3DB;
  // Other environment variables
  private String logDir, logFilename, workDir;
  private BufferedWriter logBW;
  // VAT variables
  long vatRate;
  // Standard message variable
  private String message;
  // Job request details
  private String conglomCustId, BPSD, BPED, billedProductId, billPeriodRef, billingPeriodRefMMYY, extractType;
  private int period;
  private String sourceSystemId, sourceConglomId, monthClosedInd, lastUpdateId, vatExemptInd;
  private String mthlyBillStartDate, mthlyBillEndDate, qtrlyBillStartDate, qtrlyBillEndDate;
  // run totals
  private long netTotal, vatTotal, grossTotal;
  // run recordsets
  private ResultSet tBillsRS, installChargesRS, rentalChargesRS, callinkChargesRS, sundryChargesRS, creditsSSBSRS;
  private ResultSet vpnChargesRS, callChargesRS, volumeDiscountsRS, authcodeChargesRS, easyAccessQtrlyChargesRS;
  private ResultSet easyAccessUsageChargesRS, billingNumberChargesRS, sourceInvoiceRS, creditNotesRS, viewDiscountsRS;
  // tBills file variables
  private boolean tBillsFileCreated, eofTBillsFile;
  private String tBillsFileName, tBillsFileLine;
  private BufferedReader tbBR;
  // pcb credits file variables
  private boolean pcbCreditsFileCreated, eofPCBCreditsFile;
  private String pcbCreditsFileName, pcbCreditsFileLine;
  private BufferedReader pcBR;
  private String creditNoteDate, creditNoteNumber;
  private long grossAmount;
  // source invoice variables
  private String invoiceNumber, billDate, accountNumber, systemId, checkDigit;
  private long netAmount, vatAmount, totalDue;
  // source invoice totals
  private long authcodeCharges, billNumberDiscount, billNumberTotal, callCharges, callinkCharges;
  private long easyQuarterlyCharges, easyQuarterlyChargesSum, easyUsageCharges, easyUsageChargesSum;
  private long installCharges, miscCharges, oneOffCharges, recurringCharges, rentalCharges;
  private long sourceDiscTotal, specialCharges, sundryCharges, usageCharges, vpnCharges;
  private long acctBalAmount, acctBalNetAmount, acctBalVatAmount, adjustmentTotal, creditsTotal;
  private long debitsTotal, conglomDiscTotal;
  // lead account totals
  private long laBalAmount, laBalNetAmount, laBalVatAmount, laAuthcodeCharges, laCallCharges, laCallinkCharges;
  private long laEasyUsageCharges, laInstallCharges, laRentalCharges, laSpecialCharges, laSundryCharges;
  private long laUsageCharges, laVPNCharges, laConglomDiscount, laDiscountableAmount, laDiscountToApplyNet;
  private long laDiscountToApplyGross, laDiscountToApplyVat;
  // class constants
  // Various string constants
  private static String HYPHEN = "-";
  private static String EXTRACT = "Extract";
  private static String NULL = "NULL";
  // Billing systems constants
  private static String SSVO = "SSVO";
  private static String PCBL = "PCBL";

  // initialise supplied parameters
  private goldfishExtract()
  {
    sqlServer="";
    sqlServerDb="";
    sqlServerUser="";
    sqlServerPassword="";
    oracleServer="";
    oracleDb="";
    oracleUser="";
    oraclePassword="";
    oraclePort="";
    logDir="";
    workDir="";
  }

  // main class links straight to control processing
  public static void main(String[] args)
  {
    //
    goldfishExtract ge = new goldfishExtract();
    ge.control();
  }

  // get supplied parameters from properties file
  private boolean propertiesSet()
  {
    boolean result = true;
    // get property values
    try
    {
      FileReader properties = new FileReader("goldfish.properties");
      BufferedReader buffer = new BufferedReader(properties);
      boolean eofproperties = false;
      String propline = buffer.readLine();
      String propname = propline.substring(0,8);
      int lineLength = propline.length();
      String propval = propline.substring(9,lineLength).trim();
      while (!eofproperties)
      {
        if (propname.equals("sqlSServ"))
          sqlServer = propval;
        if (propname.equals("sqlSDB  "))
          sqlServerDb = propval;
        if (propname.equals("sqlSUser"))
          sqlServerUser = propval;
        if (propname.equals("sqlSPass"))
          sqlServerPassword = propval;
        if (propname.equals("oracleDB"))
          oracleDb = propval;
        if (propname.equals("oracServ"))
          oracleServer = propval;
        if (propname.equals("oracUser"))
          oracleUser = propval;
        if (propname.equals("oracPass"))
          oraclePassword = propval;
        if (propname.equals("oracPort"))
          oraclePort = propval;
        if (propname.equals("logDir  "))
          logDir = propval;
        if (propname.equals("workDir "))
          workDir = propval;
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
    return result;
  }

  // controls goldfish extract processing
  private void control()
  {
    boolean OK = true;
    boolean emptyRun = false;
    // start processing
    if (propertiesSet())
    {
      openLogFile();
      message = "Starting processing at "+currentDT();
      System.out.println(message);
      writeToLogFile(message);
      // set up main sql server connection for update
      String url = "jdbc:AvenirDriver://"+sqlServer+":1433/"+sqlServerDb;
      try
      {
        Class.forName("net.avenir.jdbcdriver7.Driver");
        sqlConn = DriverManager.getConnection(url,sqlServerUser,sqlServerPassword);
        ssDB = new sqlServerDB(sqlConn);
        message = "   Connected to SQLServer (for update)";
        System.out.println(message);
        writeToLogFile(message);
      }
      catch( Exception e)
      {
        message = "   Error opening SQLServer (for update) : " + e.toString();
        System.out.println(message);
        writeToLogFile(message);
        OK = false;
      }
      // set up sql server connection for log messages
      try
      {
        Class.forName("net.avenir.jdbcdriver7.Driver");
        sqlConn2 = DriverManager.getConnection(url,sqlServerUser,sqlServerPassword);
        ss2DB = new sqlServerDB(sqlConn2);
        message = "   Connected to SQLServer (for logging)";
        System.out.println(message);
        writeToLogFile(message);
      }
      catch( Exception e)
      {
        message = "   Error opening SQLServer (for logging) : " + e.toString();
        System.out.println(message);
        writeToLogFile(message);
        OK = false;
      }
      // set up sql server connection for read only
      try
      {
        Class.forName("net.avenir.jdbcdriver7.Driver");
        sqlConn3 = DriverManager.getConnection(url,sqlServerUser,sqlServerPassword);
        ss3DB = new sqlServerDB(sqlConn2);
        message = "   Connected to SQLServer (for read only)";
        System.out.println(message);
        writeToLogFile(message);
      }
      catch( Exception e)
      {
        message = "   Error opening SQLServer (for read only) : " + e.toString();
        System.out.println(message);
        writeToLogFile(message);
        OK = false;
      }
      // check for any active job requests
      if (OK)
      {
        ResultSet activeJobsRS = ss3DB.getActiveJobs();
        try
        {
          while (activeJobsRS.next())
          {
            String conglomCustId = activeJobsRS.getString("Conglom_Cust_Id");
            String conglomCustName = activeJobsRS.getString("Conglom_Cust_Name");
            String billedProductId = activeJobsRS.getString("Billed_Product_Id");
            String billPeriodRef = activeJobsRS.getString("Bill_Period_Ref");
            String extractType = activeJobsRS.getString("Extract_Type");
            period = activeJobsRS.getInt("Period");
            message =
              "   Cannot proceeed as job is already running for "+
              conglomCustId+":"+
              conglomCustName+":"+
              billedProductId+":"+
              billPeriodRef+":"+
              extractType;
            System.out.println(message);
            writeToLogFile(message);
            OK = false;
          }
        }
        catch(java.sql.SQLException ex)
        {
          message = "   Error checking for active jobs : "+ex.toString();
          System.out.println(message);
          writeToLogFile(message);
        }
        activeJobsRS = null;
      }
      // start next job if nothing running
      if (OK)
      {
        long numberRequests = ss3DB.queueCount();
        if (numberRequests==0)
        {
          message = "   No outstanding goldfish extract job requests";
          System.out.println(message);
          writeToLogFile(message);
          emptyRun = true;
        }
        else
        {
          while (numberRequests>0)
          {
            if (ssDB.copyJobFromQueue())
            {
              // Get job details from BPRC and invoke extract
              ResultSet activeJobsRS = ssDB.getActiveJobs();
              try
              {
                // store current vatRate
                vatRate = ss3DB.getVatRate();
                while (activeJobsRS.next())
                {
                  String conglomCustId = activeJobsRS.getString("Conglom_Cust_Id");
                  String conglomCustName = activeJobsRS.getString("Conglom_Cust_Name");
                  String billedProductId = activeJobsRS.getString("Billed_Product_Id");
                  String billPeriodRef = activeJobsRS.getString("Bill_Period_Ref");
                  String extractType = activeJobsRS.getString("Extract_Type");
                  vatExemptInd = activeJobsRS.getString("VAT_Exempt_Ind");
                  period = activeJobsRS.getInt("Period");
                  performExtract();
                  // on completion set BPRC inactive
                  if (!ssDB.resetBPRC(conglomCustId,billedProductId))
                  {
                    OK = false;
                    message = " failed to reset BPRC for "+
                      conglomCustId+":"+
                      conglomCustName+":"+
                      billedProductId+":"+
                      billPeriodRef+":"+
                      extractType;
                    System.out.println(message);
                    writeToLogFile(message);
                    numberRequests = 0;
                  }
                }
              }
              catch(java.sql.SQLException ex)
              {
                message = "   Error checking for active jobs : "+ex.toString();
                System.out.println(message);
                writeToLogFile(message);
              }
              activeJobsRS = null;
            }
            else
            {
              message = "   Unable to proceed due to error with Copy_Job_From_Queue stored procedure";
              System.out.println(message);
              writeToLogFile(message);
              OK = false;
              numberRequests = 0;
            }
            if (OK)
              numberRequests = ss3DB.queueCount();
          }
        }
      }
      //end processing
      message = "Ending processing at "+currentDT();
      System.out.println(message);
      writeToLogFile(message);
      closeLogFile();
      // if no Customers run then delete log file
      if (emptyRun)
      {
        File logFile = new File(logFilename);
        if(!logFile.delete())
          System.out.print("Failed to delete logfile "+logFilename);
      }
      sqlConn=null;
      sqlConn2=null;
      ssDB=null;
      ss2DB=null;
    }
    else
      System.out.println("Failed to set properties - abandoning run");
  }

  // goldfish extract run(s)
  private void performExtract()
  {
    boolean OK = true, transaction = false;
    ResultSet detailsRS = ssDB.getJobDetails();
    try
    {
      if (detailsRS.next())
      {
        conglomCustId = detailsRS.getString("Conglom_Cust_Id");
        BPSD = oracleDate(detailsRS.getString("Billing_Period_Start_Date"));
        BPED = oracleDate(detailsRS.getString("Billing_Period_End_Date"));
        billedProductId = detailsRS.getString("Billed_Product_Id");
        billPeriodRef = detailsRS.getString("Bill_Period_Ref");
        billingPeriodRefMMYY = detailsRS.getString("Billing_Period_RefMMYY");
        extractType = detailsRS.getString("Extract_Type");
        sourceSystemId = detailsRS.getString("Source_System_Id");
        sourceConglomId = detailsRS.getString("Source_Conglom_Id");
        monthClosedInd = detailsRS.getString("Month_Closed_Ind");
        lastUpdateId = detailsRS.getString("Last_Update_Id");
        vatExemptInd = detailsRS.getString("Vat_Exempt_Ind");
        mthlyBillStartDate = detailsRS.getString("Mthly_Bill_Start_Date");
        mthlyBillEndDate = detailsRS.getString("Mthly_Bill_End_Date");
        qtrlyBillStartDate = detailsRS.getString("Qtrly_Bill_Start_Date");
        qtrlyBillEndDate = detailsRS.getString("Qtrly_Bill_End_Date");
        message =
          "Starting Goldfish Extract job for "+
          conglomCustId+":"+
          BPSD+":"+
          BPED+":"+
          billedProductId+":"+
          billPeriodRef;
        ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
        message = "   "+message;
        System.out.println(message);
        writeToLogFile(message);
      }
    }
    catch(java.sql.SQLException ex)
    {
      message = "   Unable to proceed due to failure to get details for the current job";
      System.out.println(message);
      writeToLogFile(message);
      OK = false;
    }
    // connect to oracle
    if (OK)
      if (openOracleConnection(1))
        oDB = new oracleDB(oracleConn);
      else
        OK = false;
    // extract source invoice details
    if (OK)
    {
      ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I","Retrieving bills");
      // get t_bills relevant to the billing product
      if (billedProductId.startsWith(SSVO))
        tBillsRS = oDB.getSSBSTBills(sourceConglomId,billingPeriodRefMMYY);
      else
      {
        if(BPED.equals(""))
          BPED = "2099-Dec-31";
        tBillsRS = oDB.getPCBTBills(sourceConglomId,BPSD,BPED);
      }
      // load t_bills recordset into file
      tBillsFileName = workDir + File.separator + "tBills.txt";
      tBillsFileCreated = createTBillsFile(tBillsRS,tBillsFileName);
      tBillsRS = null;
      closeOracleConnection(oracleConn);
      oracleConn = null;
      oDB = null;
      // Get first tBills line
      if (openTBillsFile(tBillsFileName))
      {
        tBillsFileLine = nextTBillsFileLine();
        if ((tBillsFileLine.equals("EOF"))||(tBillsFileLine.startsWith("IO")))
        {
          if (billedProductId.startsWith(SSVO))
          {
            message = "<font color=red> No Source Invoices found to extract for this period </font>";
            ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"X",message);
            String exceptionStatus = ss3DB.getExceptionStatus("E1");
            ssDB.writeTempException
              (conglomCustId,
               billedProductId,
               billPeriodRef,
               EXTRACT,
               NULL,
               NULL,
               BPSD,
               0,
               0,
               exceptionStatus,
               "E1",
               lastUpdateId);
            OK = false;
          }
          else
          {
            message =
              "E1 - No records found on T_Bills extracting for "+sourceSystemId+
              " Conglom Id "+sourceConglomId+
              " from "+BPSD+" to "+BPED;
            ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
          }
        }
        else
        {
          if (!conglomCustId.equals("XXX"))
          {
            if (extractType.startsWith("F"))
              if (!ssDB.clearSourceBills(conglomCustId,billedProductId,billPeriodRef))
              {
                message = "<font color=red>Error run procedure Clear_Source_Bills</font>";
                ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
                message = "   "+message;
                System.out.println(message);
                writeToLogFile(message);
                OK = false;
              }
            // process t_bills (note already on first one)
            if (OK)
            {
              if (!processTBills())
                OK = false;
            }
          }
          else
            if (ssDB.beginTran())
            {
              transaction = true;
              message = "   Successful issue of BEGIN TRAN";
              System.out.println(message);
              writeToLogFile(message);
              // clear all sourcebills if a full run
              if (extractType.startsWith("F"))
                if (!ssDB.clearSourceBills(conglomCustId,billedProductId,billPeriodRef))
                {
                  message = "<font color=red>Error run procedure Clear_Source_Bills</font>";
                  ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
                  message = "   "+message;
                  System.out.println(message);
                  writeToLogFile(message);
                  OK = false;
                }
              // process t_bills (note already on first one)
              if (OK)
              {
                if (!processTBills())
                  OK = false;
              }
          }
        }
      }
      // close tbills file
      closeTBillsFile();
      // Lead account discounts
      if (OK)
      {
        OK = processLeadAccountDiscounts();
      }
      // Prior period source invoice for PCB quarterly accounts
      if (OK)
      {
        if ((billedProductId.equals("PCBL"))&&(period==3))
          OK = processPriorPeriodTBills();
      }
      // PCB Credits
      if (OK)
      {
        if (billedProductId.equals("PCBL"))
          OK = processPCBCredits();
      }
      // Load local data
      if (OK)
      {
        OK = processLocalData();
      }
      // Success message
      if (OK)
      {
        message = "<font color=green><b>Goldfish extract successful </font>";
        ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
        message = "   "+message;
        System.out.println(message);
        writeToLogFile(message);
      }
      // invoke stored proc to process temp exceptions raised
      ssDB.resolveExceptions(lastUpdateId,conglomCustId,billedProductId);
    }
    // if begin transaction has been issued then commit or rollback as appropriate
    if (transaction)
      if (OK)
      {
        if (ssDB.commitTran())
        {
          message = "   Sucessful issue of COMMIT TRAN";
          System.out.println(message);
          writeToLogFile(message);
        }
        else
        {
          message = "   Error issuing COMMIT TRAN";
          ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
          System.out.println(message);
          writeToLogFile(message);
        }
      }
      else
      {
        if (ssDB.rollbackTran())
        {
          message = "   Sucessful issue of ROLLBACK TRAN";
          ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
          System.out.println(message);
          writeToLogFile(message);
        }
        else
        {
          message = "   Error issuing ROLLBACK TRAN";
          ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
          System.out.println(message);
          writeToLogFile(message);
        }
      }
      message = "Ending Goldfish Extract job";
      ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
      message = "   "+message;
      System.out.println(message);
      writeToLogFile(message);
      detailsRS = null;
  }

  // create tbills file with details of each tbills row
  private boolean createTBillsFile(ResultSet tbRS, String tbFilename)
  {
    boolean result = false, continueProcess = true;
    BufferedWriter tbBW = null;
    // Open work tBills file
    try
    {
      tbBW = new BufferedWriter(new FileWriter(tbFilename));
    }
    catch (IOException e)
    {
      System.out.println("   Error opening tBills file : "+e.toString());
      continueProcess = false;
    }
    if (continueProcess)
    {
      try
      {
        while (tbRS.next())
        {
          String wLine =
            tbRS.getString("Invoice_Number").trim()+"|"+
            tbRS.getString("Acco_Account_Number")+"|"+
            tbRS.getString("Bill_Date")+"|"+
            tbRS.getLong("Net_Amount")+"|"+
            tbRS.getLong("Vat_Amount")+"|"+
            tbRS.getLong("Total_Due")+"|"+
            tbRS.getString("Check_Digit");
          try
          {
            tbBW.write(wLine+"\r\n");
          }
          catch (IOException e)
          {
            System.out.println("   Error writing to tbills work file : "+e.toString());
          }
        }
      }
      catch (java.sql.SQLException e)
      {
        System.out.println("   Error processing tBills : "+e.toString());
        continueProcess = false;
      }
    }
    // Close work tBills file
    try
    {
      tbBW.close();
      result = true;
    }
    catch (IOException e)
    {
      System.out.println("   Error closing tBills file : "+e.toString());
    }
    return result;
  }

  // create pcb credits files with details of each credit in the file
  private boolean createPCBCreditsFile(ResultSet pcRS, String pcFilename)
  {
    boolean result = false, continueProcess = true;
    BufferedWriter pcBW = null;
    // Open work pcb credits file
    try
    {
      pcBW = new BufferedWriter(new FileWriter(pcFilename));
    }
    catch (IOException e)
    {
      System.out.println("   Error opening pcb credits file : "+e.toString());
      continueProcess = false;
    }
    if (continueProcess)
    {
      try
      {
        while (pcRS.next())
        {
          String wLine =
            pcRS.getString("Credit_Note_Date")+"|"+
            pcRS.getString("Credit_Note_Number")+"|"+
            pcRS.getLong("Net_Amount")+"|"+
            pcRS.getLong("Vat_Amount")+"|"+
            pcRS.getLong("Gross_Amount")+"|"+
            pcRS.getString("Acco_Account_Number")+"|"+
            pcRS.getString("Check_Digit");
          try
          {
            pcBW.write(wLine+"\r\n");
          }
          catch (IOException e)
          {
            System.out.println("   Error writing to pcb credits work file : "+e.toString());
          }
        }
      }
      catch (java.sql.SQLException e)
      {
        System.out.println("   Error processing pcb credits : "+e.toString());
        continueProcess = false;
      }
    }
    // Close work pcb credits file
    try
    {
      pcBW.close();
      result = true;
    }
    catch (IOException e)
    {
      System.out.println("   Error closing pcb credits file : "+e.toString());
    }
    return result;
  }

  // put tbills file data into relevant variables
  private void decodeTBillsLine(String tbLine)
  {
    int itemCount = 1;
    String work = "";
    for (int pos = 0; pos < tbLine.length() ; pos++)
    {
      String testChar = tbLine.substring(pos,pos+1);
      if (testChar.equals("|"))
      {
        switch (itemCount)
        {
          case 1 :
            invoiceNumber = work;
            break;
          case 2 :
            accountNumber = work;
            break;
          case 3 :
            billDate = work;
            break;
          case 4 :
            netAmount = Long.parseLong(work);
            break;
          case 5 :
            vatAmount = Long.parseLong(work);
            break;
          case 6 :
            totalDue = Long.parseLong(work);
            break;
        }
        itemCount++;
        work = "";
      }
      else
      {
        work = work + testChar;
      }
    }
    checkDigit = work;
    //System.out.println(invoiceNumber+":"+accountNumber+":"+billDate+":"+netAmount+":"+vatAmount+":"+totalDue+":"+checkDigit);
  }

  // put pcb credits file data into relevant variables
  private void decodePCBCreditsLine(String pcLine)
  {
    int itemCount = 1;
    String work = "";
    for (int pos = 0; pos < pcLine.length() ; pos++)
    {
      String testChar = pcLine.substring(pos,pos+1);
      if (testChar.equals("|"))
      {
        switch (itemCount)
        {
          case 1 :
            creditNoteDate = work;
            break;
          case 2 :
            creditNoteNumber = work;
            break;
          case 3 :
            netAmount = Long.parseLong(work);
            break;
          case 4 :
            vatAmount = Long.parseLong(work);
            break;
          case 5 :
            grossAmount = Long.parseLong(work);
            break;
          case 6 :
            accountNumber = work;
            break;
        }
        itemCount++;
        work = "";
      }
      else
      {
        work = work + testChar;
      }
    }
    checkDigit = work;
    //System.out.println(creditNoteDate+":"+creditNoteNumber+":"+netAmount+":"+vatAmount+":"+grossAmount+":"+accountNumber+":"+checkDigit);
  }

  // control processing of tbills
  private boolean processTBills()
  {
    boolean result = false, continueProcess = true;
    // Process first t_bills
    decodeTBillsLine(tBillsFileLine);
    // set totals to zero
    netTotal = 0;
    vatTotal = 0;
    grossTotal = 0;
    message = "Start of extract for Source Invoice No: "+invoiceNumber;
    ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
    message = "   "+message;
    System.out.println(message);
    writeToLogFile(message);
    initSourceInvoiceTotals();
    // only reset lead account totals for the first source account
    initLeadAccountTotals();
    if (extractType.startsWith("F"))
      processSourceInvoiceCharges();
    else if (!ssDB.sourceInvoiceExists(conglomCustId,billedProductId,invoiceNumber))
      processSourceInvoiceCharges();
    tBillsFileLine = nextTBillsFileLine();
    while (((!tBillsFileLine.equals("EOF"))&&(continueProcess)&&(!tBillsFileLine.startsWith("IO"))))
    {
      decodeTBillsLine(tBillsFileLine);
      message = "Start of extract for Source Invoice No: "+invoiceNumber;
      ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
      message = "   "+message;
      System.out.println(message);
      writeToLogFile(message);
      initSourceInvoiceTotals();
      // process source invoice charges unless a partial extract and it already exists
      if (extractType.startsWith("F"))
        continueProcess=processSourceInvoiceCharges();
      else if (!ssDB.sourceInvoiceExists(conglomCustId,billedProductId,invoiceNumber))
        continueProcess=processSourceInvoiceCharges();
      tBillsFileLine = nextTBillsFileLine();
    }
    if (continueProcess)
      result = true;
    return result;
  }

  // process various source invoice charges for a tbill
  private boolean processSourceInvoiceCharges()
  {
    boolean continueProcess = true;
    // connect to oracle
    if (openOracleConnection(2))
      o2DB = new oracleDB(oracleConn2);
    else
      continueProcess = false;
    if (continueProcess)
    {
      // Install Charges
      if (sourceSystemId.equals("PCB "))
        installChargesRS = o2DB.getPCBInstallCharges(sourceConglomId,invoiceNumber,billingPeriodRefMMYY,accountNumber,BPSD,BPED);
      else
        installChargesRS = o2DB.getSSBSInstallCharges(sourceConglomId,invoiceNumber,billingPeriodRefMMYY,accountNumber);
      try
      {
        while(installChargesRS.next())
        {
          // store details for T_Install_Charges
          long installationCharge = installChargesRS.getLong("Installation_Charge");
          String installationDate = installChargesRS.getString("Installation_Date");
          String equipmentCode = installChargesRS.getString("Equipment_Code");
          long equipmentQuantity = installChargesRS.getLong("Equipment_Quantity");
          String equipmentCodeDescription = installChargesRS.getString("Equipment_Code_Description");
          String circuitReference = installChargesRS.getString("Circuit_Reference");
          String contractNumber = installChargesRS.getString("Contract_Number");
          String chargeType = "";
          if (sourceSystemId.equals("PCB "))
            chargeType = "PINS";
          else
            chargeType = "VINS";
          // increment running totals
          installCharges = installCharges + installationCharge;
          oneOffCharges = oneOffCharges + installationCharge;
          laInstallCharges = laInstallCharges + installationCharge;
          // insert one_off_charge
          String result =
            ssDB.insertOneOffCharge
            (conglomCustId,
             billedProductId,
             billPeriodRef,
             invoiceNumber,
             accountNumber,
             chargeType,
             installationDate,
             contractNumber,
             circuitReference,
             equipmentCodeDescription,
             equipmentQuantity,
             installationCharge,
             lastUpdateId);
          if (!result.equals("OK"))
          {
            message = "<font color=red> Problem inserting into Source_One_Off_Charge : "+result+" </font>";
            ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
            message = "   "+message;
            System.out.println(message);
            writeToLogFile(message);
            continueProcess = false;
          }
        }
      }
      catch (java.sql.SQLException ex)
      {
        message = "Failure geting next installChargesRS : "+ex.getMessage();
        ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
        message = "   "+message;
        System.out.println(message);
        writeToLogFile(message);
        continueProcess = false;
      }
      try
      {
        installChargesRS.close();
        installChargesRS = null;
      }
      catch (java.sql.SQLException ex)
      {
        message = "Failure closing installChargesRS : "+ex.getMessage();
        ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
        message = "   "+message;
        System.out.println(message);
        writeToLogFile(message);
        continueProcess = false;
      }
    }
    installChargesRS = null;
    closeOracleConnection(oracleConn2);
    oracleConn2=null;
    o2DB = null;
    // connect to oracle
    if (continueProcess)
      if (openOracleConnection(2))
        o2DB = new oracleDB(oracleConn2);
      else
        continueProcess = false;
    if (continueProcess)
    {
      // Rental Charges
      if (sourceSystemId.equals("PCB "))
        rentalChargesRS = o2DB.getPCBRentalCharges(sourceConglomId,invoiceNumber,billingPeriodRefMMYY,accountNumber,BPSD,BPED);
      else
        rentalChargesRS = o2DB.getSSBSRentalCharges(sourceConglomId,invoiceNumber,billingPeriodRefMMYY,accountNumber);
      try
      {
        while(rentalChargesRS.next())
        {
          // store details for T_Equipment_Charges\T_Previous Charges
          long equipmentRentalAmount = rentalChargesRS.getLong("Equipment_Rental_Amount");
          String equipmentStartDate = rentalChargesRS.getString("Equipment_Start_Date");
          String equipmentStopDate = rentalChargesRS.getString("Equipment_Stop_Date");
          long equipmentQuantity = rentalChargesRS.getLong("Equipment_Quantity");
          String equipmentCodeDescription = rentalChargesRS.getString("Equipment_Code_Description");
          long vatAmount = rentalChargesRS.getLong("VAT_Amount");
          long totalAmount = rentalChargesRS.getLong("Total_Amount");
          if (sourceSystemId.equals("PCB "))
          {
            // update vat amount for PCB
            if (vatExemptInd.equals("Y"))
              vatAmount = 0;
            else
              vatAmount = (equipmentRentalAmount * vatRate)/100;
          }
          else
            // match SSBS VAT amount to 3dp
            vatAmount = vatAmount * 10;
          String aEndAddress = rentalChargesRS.getString("A_End_Address");
          String bEndAddress = rentalChargesRS.getString("B_End_Address");
          String contractNumber = rentalChargesRS.getString("Contract_Number");
          String circuitLength = rentalChargesRS.getString("Circuit_Length");
          String circuitReference = rentalChargesRS.getString("Circuit_Reference");
          String chargeType = "";
          if (sourceSystemId.equals("PCB "))
            chargeType = "PREN";
          else
            chargeType = "VREN";
          // increment running totals
          rentalCharges = rentalCharges + equipmentRentalAmount;
          recurringCharges = recurringCharges + equipmentRentalAmount;
          laRentalCharges = laRentalCharges + equipmentRentalAmount;
          // insert into sourceRecurringCharge
          String result =
            ssDB.insertRecurringCharge(
              conglomCustId,
              billedProductId,
              billPeriodRef,
              invoiceNumber,
              accountNumber,
              chargeType,
              equipmentStartDate,
              equipmentStopDate,
              equipmentCodeDescription,
              equipmentQuantity,
              equipmentRentalAmount,
              vatAmount,
              lastUpdateId,
              contractNumber,
              circuitReference,
              aEndAddress,
              bEndAddress,
              circuitLength,
              vatExemptInd);
          if (!result.equals("OK"))
          {
            message = "<font color=red> Problem inserting into Source_Recurring_Charge : "+result+" </font>";
            ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
            message = "   "+message;
            System.out.println(message);
            writeToLogFile(message);
            continueProcess = false;
          }
        }
      }
      catch (java.sql.SQLException ex)
      {
        message = "Failure geting next rentalChargesRS : "+ex.getMessage();
        ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
        message = "   "+message;
        System.out.println(message);
        writeToLogFile(message);
        continueProcess = false;
      }
      try
      {
        rentalChargesRS.close();
        rentalChargesRS = null;
      }
      catch (java.sql.SQLException ex)
      {
        message = "Failure closing rentalChargesRS : "+ex.getMessage();
        ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
        message = "   "+message;
        System.out.println(message);
        writeToLogFile(message);
        continueProcess = false;
      }
    }
    rentalChargesRS = null;
    closeOracleConnection(oracleConn2);
    oracleConn2=null;
    o2DB = null;
    if (continueProcess)
      if (sourceSystemId.equals("SSBS"))
        if (openOracleConnection(2))
          o2DB = new oracleDB(oracleConn2);
        else
          continueProcess = false;
    if (continueProcess)
    {
      // Callink Charges
      if (sourceSystemId.equals("SSBS"))
      {
        callinkChargesRS = o2DB.getSSBSCallinkCharges(sourceConglomId,invoiceNumber,billingPeriodRefMMYY,accountNumber);
        try
        {
          while (callinkChargesRS.next())
          {
            // store callink charge details
            long chargeId = callinkChargesRS.getLong("Charge_Id");
            String chargeDescription = callinkChargesRS.getString("Charge_Description");
            long chargeAmount = callinkChargesRS.getLong("Charge_Amount");
            String chargeStartDate = callinkChargesRS.getString("Charge_Start_Date");
            String chargeEndDate = callinkChargesRS.getString("Charge_End_Date");
            String chargeType = "VCAL";
            // increment the running totals
            callinkCharges = callinkCharges+chargeAmount;
            laCallinkCharges = laCallinkCharges+chargeAmount;
            usageCharges = usageCharges+chargeAmount;
            laUsageCharges = laUsageCharges+chargeAmount;
            // insert into source usage other charge
            String result = ssDB.insertCallinkCharge
              (conglomCustId,
               billedProductId,
               billPeriodRef,
               invoiceNumber,
               accountNumber,
               chargeType,
               chargeId,
               chargeDescription,
               chargeAmount,
               lastUpdateId,
               chargeStartDate,
               chargeEndDate);
            if (!result.equals("OK"))
            {
               message = "<font color=red> Problem inserting into Source_Usage_Other_Charge (callink) : "+result+" </font>";
               ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
               message = "   "+message;
               System.out.println(message);
               writeToLogFile(message);
               continueProcess = false;
            }
          }
        }
        catch (java.sql.SQLException ex)
        {
          message = "Failure geting next callinkChargesRS : "+ex.getMessage();
          ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
          message = "   "+message;
          System.out.println(message);
          writeToLogFile(message);
          continueProcess = false;
        }
        try
        {
          callinkChargesRS.close();
          callinkChargesRS = null;
        }
        catch (java.sql.SQLException ex)
        {
          message = "Failure closing callinkChargesRS : "+ex.getMessage();
          ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
          message = "   "+message;
          System.out.println(message);
          writeToLogFile(message);
          continueProcess = false;
        }
      }
    }
    callinkChargesRS = null;
    closeOracleConnection(oracleConn2);
    oracleConn2=null;
    o2DB = null;
    if (continueProcess)
    {
      if (openOracleConnection(2))
        o2DB = new oracleDB(oracleConn2);
      else
        continueProcess = false;
    }
    if (continueProcess)
    {
      // Sundry charges
      if (sourceSystemId.equals("SSBS"))
        sundryChargesRS=o2DB.getSSBSSundryCharges(sourceConglomId,invoiceNumber,billingPeriodRefMMYY,accountNumber);
      else
        sundryChargesRS=o2DB.getPCBSundryCharges(sourceConglomId,invoiceNumber,billingPeriodRefMMYY,accountNumber,BPSD,BPED);
      try
      {
        while (sundryChargesRS.next())
        {
          // get sundry charge details
          long chargeAmount = sundryChargesRS.getLong("Charge_Amount");
          String sundryCode = sundryChargesRS.getString("Sundry_Code");
          String sundryDescription = sundryChargesRS.getString("Sundry_Description");
          String chargeType = "";
          if (sourceSystemId.equals("SSBS"))
            chargeType = "VSUN";
          else
            chargeType = "PSUN";
          // increment totals
          sundryCharges = sundryCharges + chargeAmount;
          laSundryCharges = laSundryCharges + chargeAmount;
          miscCharges = miscCharges + chargeAmount;
          // insert into source sundry charge
          String result = ssDB.insertSundryCharge
            (conglomCustId,
             billedProductId,
             billPeriodRef,
             invoiceNumber,
             accountNumber,
             chargeType,
             sundryCode,
             sundryDescription,
             chargeAmount,
             lastUpdateId);
          if (!result.equals("OK"))
          {
             message = "<font color=red> Problem inserting into Source_Sundry_Charge : "+result+" </font>";
             ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
             message = "   "+message;
             System.out.println(message);
             writeToLogFile(message);
             continueProcess = false;
          }
        }
        try
        {
          sundryChargesRS.close();
          sundryChargesRS = null;
        }
        catch (java.sql.SQLException ex)
        {
          message = "Failure closing sundryChargesRS : "+ex.getMessage();
          ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
          message = "   "+message;
          System.out.println(message);
          writeToLogFile(message);
          continueProcess = false;
        }
      }
      catch (java.sql.SQLException ex)
      {
        message = "Failure geting next sundryChargesRS : "+ex.getMessage();
        ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
        message = "   "+message;
        System.out.println(message);
        writeToLogFile(message);
        continueProcess = false;
      }
    }
    sundryChargesRS=null;
    closeOracleConnection(oracleConn2);
    oracleConn2=null;
    o2DB=null;
    if (continueProcess)
      if (sourceSystemId.equals("SSBS"))
        if (openOracleConnection(2))
          o2DB = new oracleDB(oracleConn2);
        else
          continueProcess = false;
    if (continueProcess)
    {
      // VPN Charges
      if (sourceSystemId.equals("SSBS"))
      {
        vpnChargesRS = o2DB.getSSBSVPNCharges(sourceConglomId,invoiceNumber,billingPeriodRefMMYY,accountNumber);
        try
        {
          while (vpnChargesRS.next())
          {
            // store vpn charge details
            String vpnStartDate = vpnChargesRS.getString("VPN_Start_Date");
            String vpnStopDate = vpnChargesRS.getString("VPN_Stop_Date");
            long vpnChargeAmount = vpnChargesRS.getLong("VPN_Charge_Amount");
            String chargeType = "VVPN";
            // increment totals
            vpnCharges = vpnCharges + vpnChargeAmount;
            laVPNCharges = laVPNCharges + vpnChargeAmount;
            usageCharges = usageCharges + vpnChargeAmount;
            laUsageCharges = laUsageCharges + vpnChargeAmount;
            String result = ssDB.insertVPNCharge
              (conglomCustId,
               billedProductId,
               billPeriodRef,
               invoiceNumber,
               accountNumber,
               chargeType,
               vpnStartDate,
               vpnStopDate,
               vpnChargeAmount,
               lastUpdateId);
            if (!result.equals("OK"))
            {
               message = "<font color=red> Problem inserting into Source_Usage_Summary (VPN) : "+result+" </font>";
               ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
               message = "   "+message;
               System.out.println(message);
               writeToLogFile(message);
               continueProcess = false;
            }
          }
        }
        catch (java.sql.SQLException ex)
        {
          message = "Failure geting next SSBS VPN Charge : "+ex.getMessage();
          ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
          message = "   "+message;
          System.out.println(message);
          writeToLogFile(message);
          continueProcess = false;
        }
        try
        {
          vpnChargesRS.close();
          vpnChargesRS = null;
        }
        catch (java.sql.SQLException ex)
        {
          message = "Failure closing vpnChargeRS : "+ex.getMessage();
          ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
          message = "   "+message;
          System.out.println(message);
          writeToLogFile(message);
          continueProcess = false;
        }
      }
    }
    vpnChargesRS = null;
    closeOracleConnection(oracleConn2);
    oracleConn2=null;
    o2DB = null;
    if (continueProcess)
      if (sourceSystemId.equals("SSBS"))
        if (openOracleConnection(2))
          o2DB = new oracleDB(oracleConn2);
        else
          continueProcess = false;
    if (continueProcess)
    {
      // Call Charges
      if (sourceSystemId.equals("SSBS"))
      {
        callChargesRS = o2DB.getSSBSCallCharges(sourceConglomId,invoiceNumber,billingPeriodRefMMYY,accountNumber);
        try
        {
          while (callChargesRS.next())
          {
            // store call charge details
            String currentStartDate = callChargesRS.getString("Current_Start_Date");
            String currentStopDate = callChargesRS.getString("Current_Stop_Date");
            long currentCharge = callChargesRS.getLong("Current_Charge");
            String previousStartDate = callChargesRS.getString("Previous_Start_Date");
            String previousStopDate = callChargesRS.getString("Previous_Stop_Date");
            long previousCharge = callChargesRS.getLong("Previous_Charge");
            String chargeType = "VPST";
            // increment totals
            callCharges = callCharges + currentCharge + previousCharge;
            laCallCharges = laCallCharges + currentCharge + previousCharge;
            usageCharges = usageCharges + currentCharge + previousCharge;
            laUsageCharges = laUsageCharges + currentCharge + previousCharge;;
            String result = ssDB.insertCallCharge
              (conglomCustId,
               billedProductId,
               billPeriodRef,
               invoiceNumber,
               accountNumber,
               chargeType,
               currentStartDate,
               currentStopDate,
               previousStartDate,
               previousStopDate,
               currentCharge,
               previousCharge,
               lastUpdateId);
            if (!result.equals("OK"))
            {
               message = "<font color=red> Problem inserting into Source_Usage_Summary (Call Charges) : "+result+" </font>";
               ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
               message = "   "+message;
               System.out.println(message);
               writeToLogFile(message);
               continueProcess = false;
            }
          }
        }
        catch (java.sql.SQLException ex)
        {
          message = "Failure geting next SSBS Call Charge : "+ex.getMessage();
          ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
          message = "   "+message;
          System.out.println(message);
          writeToLogFile(message);
          continueProcess = false;
        }
        try
        {
          callChargesRS.close();
          callChargesRS = null;
        }
        catch (java.sql.SQLException ex)
        {
          message = "Failure closing callChargeRS : "+ex.getMessage();
          ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
          message = "   "+message;
          System.out.println(message);
          writeToLogFile(message);
          continueProcess = false;
        }
      }
    }
    callChargesRS = null;
    closeOracleConnection(oracleConn2);
    oracleConn2=null;
    o2DB = null;
    if (continueProcess)
      if (sourceSystemId.equals("SSBS"))
        if (openOracleConnection(2))
          o2DB = new oracleDB(oracleConn2);
        else
          continueProcess = false;
    if (continueProcess)
    {
      // Volume Discounts
      if (sourceSystemId.equals("SSBS"))
      {
        volumeDiscountsRS = o2DB.getSSBSVolumeDiscounts(sourceConglomId,invoiceNumber,billingPeriodRefMMYY,accountNumber);
        try
        {
          while (volumeDiscountsRS.next())
          {
            // store volume discount details
            String billText = volumeDiscountsRS.getString("Bill_Text");
            String subBillText = volumeDiscountsRS.getString("Sub_Bill_Text");
            long discount = volumeDiscountsRS.getLong("Discount");
            String chargeType = "VVOL";
            // increment totals
            sourceDiscTotal = sourceDiscTotal + discount;
            String result = ssDB.insertVolumeDiscount
              (conglomCustId,
               billedProductId,
               billPeriodRef,
               invoiceNumber,
               accountNumber,
               chargeType,
               billText,
               subBillText,
               discount,
               lastUpdateId);
            if (!result.equals("OK"))
            {
               message = "<font color=red> Problem inserting into Source_Volume_Dscount : "+result+" </font>";
               ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
               message = "   "+message;
               System.out.println(message);
               writeToLogFile(message);
               continueProcess = false;
            }
          }
        }
        catch (java.sql.SQLException ex)
        {
          message = "Failure geting next SSBS Volume Discount : "+ex.getMessage();
          ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
          message = "   "+message;
          System.out.println(message);
          writeToLogFile(message);
          continueProcess = false;
        }
        try
        {
          volumeDiscountsRS.close();
          volumeDiscountsRS = null;
        }
        catch (java.sql.SQLException ex)
        {
          message = "Failure closing volumeDiscountsRS : "+ex.getMessage();
          ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
          message = "   "+message;
          System.out.println(message);
          writeToLogFile(message);
          continueProcess = false;
        }
      }
    }
    volumeDiscountsRS = null;
    closeOracleConnection(oracleConn2);
    oracleConn2=null;
    o2DB = null;
    if (continueProcess)
      if (sourceSystemId.equals("SSBS"))
        if (openOracleConnection(2))
          o2DB = new oracleDB(oracleConn2);
        else
          continueProcess = false;
    if (continueProcess)
    {
      // Authcode charges
      if (sourceSystemId.equals("SSBS"))
      {
        authcodeChargesRS = o2DB.getSSBSAuthcodeCharges(sourceConglomId,invoiceNumber,billingPeriodRefMMYY,accountNumber);
        try
        {
          while (authcodeChargesRS.next())
          {
            // store volume discount details
            String authcodeDate = authcodeChargesRS.getString("Authcode_Date");
            String chargeDescription = authcodeChargesRS.getString("Charge_Description");
            long authcodeCharge = authcodeChargesRS.getLong("Authcode_Charge");
            String chargeType = "";
            if (chargeDescription.startsWith("AUTH"))
            {
              chargeType = "VATH";
              // increment totals
              authcodeCharges = authcodeCharges + authcodeCharge;
              laAuthcodeCharges = laAuthcodeCharges + authcodeCharge;
              usageCharges = usageCharges + authcodeCharge;
              laUsageCharges = laUsageCharges + authcodeCharge;
            }
            else
            {
              chargeType = "VSPE";
              // increment totals
              specialCharges = specialCharges + authcodeCharge;
              laSpecialCharges = laSpecialCharges + authcodeCharge;
              miscCharges = miscCharges + authcodeCharge;
            }
            String result = ssDB.insertAuthcodeCharge
              (conglomCustId,
               billedProductId,
               billPeriodRef,
               invoiceNumber,
               accountNumber,
               chargeType,
               authcodeDate,
               chargeDescription,
               authcodeCharge,
               lastUpdateId);
            if (!result.equals("OK"))
            {
               message =
                "<font color=red> Problem inserting into either Source_Usage_Other_Charge or "+
                "Source Sundry_ChHarge (Authcode_Charges) : "+result+" </font>";
               ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
               message = "   "+message;
               System.out.println(message);
               writeToLogFile(message);
               continueProcess = false;
            }
          }
        }
        catch (java.sql.SQLException ex)
        {
          message = "Failure geting next SSBS Authcode Charge : "+ex.getMessage();
          ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
          message = "   "+message;
          System.out.println(message);
          writeToLogFile(message);
          continueProcess = false;
        }
        try
        {
          authcodeChargesRS.close();
          authcodeChargesRS = null;
        }
        catch (java.sql.SQLException ex)
        {
          message = "Failure closing authcodeChargesRS : "+ex.getMessage();
          ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
          message = "   "+message;
          System.out.println(message);
          writeToLogFile(message);
          continueProcess = false;
        }
      }
    }
    authcodeChargesRS = null;
    closeOracleConnection(oracleConn2);
    oracleConn2=null;
    o2DB = null;
    if (continueProcess)
      if (sourceSystemId.equals("SSBS"))
        if (openOracleConnection(2))
          o2DB = new oracleDB(oracleConn2);
        else
          continueProcess = false;
    if (continueProcess)
    {
      // Easy Access Quarterly Charges
      if (sourceSystemId.equals("SSBS"))
      {
        easyAccessQtrlyChargesRS = o2DB.getSSBSEasyAccessQtrlyCharges(sourceConglomId,invoiceNumber,billingPeriodRefMMYY,accountNumber);
        try
        {
          while (easyAccessQtrlyChargesRS.next())
          {
            // store Easy Access Quarterly Charges details
            long quarterlyCharge = easyAccessQtrlyChargesRS.getLong("Quarterly_Charge");
            String quarterlyChargeDate = easyAccessQtrlyChargesRS.getString("Quarterly_Charge_Date");
            long quarterlyDiscountAmount = easyAccessQtrlyChargesRS.getLong("Quarterly_Discount_Amount");
            String quarterlyDiscountDate = easyAccessQtrlyChargesRS.getString("Quarterly_Discount_Date");
            // increment totals
            easyQuarterlyCharges = easyQuarterlyCharges + quarterlyCharge +quarterlyDiscountAmount;
            recurringCharges = recurringCharges + quarterlyCharge +quarterlyDiscountAmount;
            String result = ssDB.insertEasyAccessQtrlyCharge
              (conglomCustId,
               billedProductId,
               billPeriodRef,
               invoiceNumber,
               accountNumber,
               quarterlyChargeDate,
               quarterlyCharge,
               quarterlyDiscountDate,
               quarterlyDiscountAmount,
               lastUpdateId);
            if (!result.equals("OK"))
            {
               message = "<font color=red> Problem inserting into Source_EasyAcc_Qtrly_Charge : "+result+" </font>";
               ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
               message = "   "+message;
               System.out.println(message);
               writeToLogFile(message);
               continueProcess = false;
            }
          }
        }
        catch (java.sql.SQLException ex)
        {
          message = "Failure geting next SSBS Easy Access Quarterly Charges : "+ex.getMessage();
          ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
          message = "   "+message;
          System.out.println(message);
          writeToLogFile(message);
          continueProcess = false;
        }
        try
        {
          easyAccessQtrlyChargesRS.close();
          easyAccessQtrlyChargesRS = null;
        }
        catch (java.sql.SQLException ex)
        {
          message = "Failure closing easyAccessQtrlyChargesRS : "+ex.getMessage();
          ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
          message = "   "+message;
          System.out.println(message);
          writeToLogFile(message);
          continueProcess = false;
        }
      }
    }
    easyAccessQtrlyChargesRS = null;
    closeOracleConnection(oracleConn2);
    oracleConn2=null;
    o2DB = null;
    if (continueProcess)
      if (sourceSystemId.equals("SSBS"))
        if (openOracleConnection(2))
          o2DB = new oracleDB(oracleConn2);
        else
          continueProcess = false;
    if (continueProcess)
    {
      // Easy Access Usage Charges
      if (sourceSystemId.equals("SSBS"))
      {
        easyAccessUsageChargesRS = o2DB.getSSBSEasyAccessUsageCharges(sourceConglomId,invoiceNumber,billingPeriodRefMMYY,accountNumber);
        try
        {
          while (easyAccessUsageChargesRS.next())
          {
            // store Easy Access Usage Charges details
            long previousPeriodAmount = easyAccessUsageChargesRS.getLong("Previous_Period_Amount");
            String previousPeriodStartDate = easyAccessUsageChargesRS.getString("Previous_Period_Start_Date");
            String previousPeriodStopDate = easyAccessUsageChargesRS.getString("Previous_Period_Stop_Date");
            long currentPeriodCharge = easyAccessUsageChargesRS.getLong("Current_Period_Charge");
            String currentPeriodStartDate = easyAccessUsageChargesRS.getString("Current_Period_Start_Date");
            String currentPeriodStopDate = easyAccessUsageChargesRS.getString("Current_Period_Stop_Date");
            String chargeType = "VESY";
            // increment totals
            easyUsageCharges = easyUsageCharges + previousPeriodAmount + currentPeriodCharge;
            laEasyUsageCharges = laEasyUsageCharges + previousPeriodAmount + currentPeriodCharge;
            usageCharges = usageCharges + previousPeriodAmount + currentPeriodCharge;
            laUsageCharges = laUsageCharges + previousPeriodAmount + currentPeriodCharge;
            String result = ssDB.insertEasyAccessUsageCharge
              (conglomCustId,
               billedProductId,
               billPeriodRef,
               invoiceNumber,
               accountNumber,
               chargeType,
               previousPeriodAmount,
               previousPeriodStartDate,
               previousPeriodStopDate,
               currentPeriodCharge,
               currentPeriodStartDate,
               currentPeriodStopDate,
               lastUpdateId);
            if (!result.equals("OK"))
            {
               message = "<font color=red> Problem inserting into Source_Usage_Summary : "+result+" </font>";
               ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
               message = "   "+message;
               System.out.println(message);
               writeToLogFile(message);
               continueProcess = false;
            }
          }
        }
        catch (java.sql.SQLException ex)
        {
          message = "Failure geting next SSBS Easy Access Usage Charges : "+ex.getMessage();
          ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
          message = "   "+message;
          System.out.println(message);
          writeToLogFile(message);
          continueProcess = false;
        }
        try
        {
          easyAccessUsageChargesRS.close();
          easyAccessUsageChargesRS = null;
        }
        catch (java.sql.SQLException ex)
        {
          message = "Failure closing easyAccessUsageyChargesRS : "+ex.getMessage();
          ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
          message = "   "+message;
          System.out.println(message);
          writeToLogFile(message);
          continueProcess = false;
        }
      }
    }
    easyAccessUsageChargesRS = null;
    closeOracleConnection(oracleConn2);
    oracleConn2=null;
    o2DB = null;
    if (continueProcess)
      if (sourceSystemId.equals("SSBS"))
        if (openOracleConnection(2))
          o2DB = new oracleDB(oracleConn2);
        else
          continueProcess = false;
    if (continueProcess)
    {
      // Billing Number Charges
      if (sourceSystemId.equals("SSBS"))
      {
        billingNumberChargesRS = o2DB.getSSBSBillingNumberCharges(sourceConglomId,invoiceNumber,billingPeriodRefMMYY);
        // calculate discount charge value
        long discountCharges = vpnCharges + easyUsageCharges + callCharges;
        try
        {
          while (billingNumberChargesRS.next())
          {
            // store Billing Number Charges details
            String billingNumber = billingNumberChargesRS.getString("Binu_Billing_Number");
            String gvpnIndicator = billingNumberChargesRS.getString("GVPN_Indicator");
            String costCentre = billingNumberChargesRS.getString("Cost_Centre");
            long durationMinutes = billingNumberChargesRS.getLong("Duration_Minutes");
            long durationSeconds = billingNumberChargesRS.getLong("Duration_Seconds");
            long numberOfCalls = billingNumberChargesRS.getLong("Number_Of_Calls");
            long billingNumberCharge = billingNumberChargesRS.getLong("Billing_Number_Charge");
            // ignore records without a billing number
            if (!(billingNumber==null))
            {
              String result = ssDB.insertBillingNumberCharge
                (conglomCustId,
                 billedProductId,
                 billPeriodRef,
                 invoiceNumber,
                 accountNumber,
                 billingNumber,
                 gvpnIndicator,
                 costCentre,
                 durationMinutes,
                 durationSeconds,
                 numberOfCalls,
                 billingNumberCharge,
                 discountCharges,
                 sourceDiscTotal,
                 lastUpdateId);
              if (!result.equals("OK"))
              {
                 message = "<font color=red> Problem inserting into Source_Bill_Number_Summary : "+result+" </font>";
                 ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
                 message = "   "+message;
                 System.out.println(message);
                 writeToLogFile(message);
                 continueProcess = false;
              }
            }
          }
        }
        catch (java.sql.SQLException ex)
        {
          message = "Failure geting next SSBS Billing Number Charges : "+ex.getMessage();
          ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
          message = "   "+message;
          System.out.println(message);
          writeToLogFile(message);
          continueProcess = false;
        }
        try
        {
          billingNumberChargesRS.close();
          billingNumberChargesRS = null;
        }
        catch (java.sql.SQLException ex)
        {
          message = "Failure closing billingNumberChargesRS : "+ex.getMessage();
          ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
          message = "   "+message;
          System.out.println(message);
          writeToLogFile(message);
          continueProcess = false;
        }
      }
    }
    billingNumberChargesRS = null;
    closeOracleConnection(oracleConn2);
    oracleConn2=null;
    o2DB = null;
    if (continueProcess)
    {
      // Discounts Individual
      viewDiscountsRS=ss3DB.getViewDiscounts(conglomCustId,billedProductId,mthlyBillStartDate);
      try
      {
        while(viewDiscountsRS.next())
        {
          String discountItemCode = viewDiscountsRS.getString("Discount_Item_Code");
          long wholeValuePercent = viewDiscountsRS.getLong("Whole_Value_Percent");
          long discountRate = viewDiscountsRS.getLong("Discount_Rate");
          String leadAccountId = viewDiscountsRS.getString("Lead_Account_Id");
          String leadAccountCheckDigit = viewDiscountsRS.getString("Lead_Acct_Check_Digit");
          String acctsExcludedInd = viewDiscountsRS.getString("Accts_Excluded_Ind");
          String discountTypeDesc = viewDiscountsRS.getString("Discount_Type_Decs");
          String shortDescription = viewDiscountsRS.getString("Short_Description");
          int seqNo = viewDiscountsRS.getInt("Seq_No");
          boolean skipAccount = false;
          boolean excludedAccount = false;
          // excluded account check
          if (acctsExcludedInd.equals("Y"))
            excludedAccount = ssDB.excludedAccount(conglomCustId,billedProductId,discountItemCode,accountNumber);
          if (excludedAccount)
          {
            if ((leadAccountId.equals(accountNumber)))
            {
              // if excluded account and currently account is lead account subtract
              // current invoice totals from lead account totals
              laEasyUsageCharges = laEasyUsageCharges - easyUsageCharges;
              laAuthcodeCharges = laAuthcodeCharges - authcodeCharges;
              laCallCharges = laCallCharges - callCharges;
              laEasyUsageCharges = laEasyUsageCharges - easyUsageCharges;
              laCallinkCharges = laCallinkCharges - callinkCharges;
              laInstallCharges = laInstallCharges - installCharges;
              laRentalCharges = laRentalCharges - rentalCharges;
              laSpecialCharges = laSpecialCharges - specialCharges;
              laSundryCharges = laSundryCharges - sundryCharges;
              laUsageCharges = laUsageCharges - usageCharges;
              laVPNCharges = laVPNCharges - vpnCharges;
            }
          }
          else
          {
            if (!leadAccountId.equals(accountNumber))
            {
              // if not lead account then perform discount processing
              long discountableAmount = 0;
              if (shortDescription.equals("Installs"))
                discountableAmount = installCharges;
              else if (shortDescription.equals("Rentals"))
                discountableAmount = rentalCharges;
              else if (shortDescription.equals("All Usage"))
                discountableAmount = usageCharges;
              else if (shortDescription.equals("Callink"))
                discountableAmount = callinkCharges;
              else if (shortDescription.equals("VPN"))
                discountableAmount = vpnCharges;
              else if (shortDescription.equals("Call"))
                discountableAmount = callCharges;
              else if (shortDescription.equals("Authcode"))
                discountableAmount = authcodeCharges;
              else if (shortDescription.equals("Sundry"))
                discountableAmount = sundryCharges;
              else if (shortDescription.equals("Special"))
                discountableAmount = specialCharges;
              else if (shortDescription.equals("Easy Acc Usage"))
                discountableAmount = easyUsageCharges;
              // proceed if discountable amount is not zero
              if (discountableAmount!=0)
              {
                long dNet = ( discountableAmount * wholeValuePercent * discountRate * -1 ) / 10000000;
                long dVat = 0;
                if (!vatExemptInd.equals("Y"))
                  dVat = ( dNet * vatRate ) / 1000;
                long discountNet = ss3DB.round3DecTo2(dNet);
                long discountVat = ss3DB.round3DecTo2(dVat);
                long discountGross = discountNet + discountVat;
                // increment totals
                conglomDiscTotal = conglomDiscTotal + discountNet;
                acctBalNetAmount = acctBalNetAmount + discountNet;
                acctBalVatAmount = acctBalVatAmount + discountVat;
                acctBalAmount = acctBalAmount + discountGross;
                // check for existing conglom discount
                ResultSet conglomDiscountRS =
                  ss3DB.getConglomDiscount
                    (conglomCustId,billedProductId,billPeriodRef,accountNumber);
                try
                {
                  if (conglomDiscountRS.next())
                  {
                    String result = "NO MATCH";
                    // does exist so update
                    long netAmount = conglomDiscountRS.getLong("Net_Amount") + discountNet;
                    long vatAmount = conglomDiscountRS.getLong("VAT Amount") + discountVat;
                    long totalAmount = conglomDiscountRS.getLong("Total_Amount") +discountGross;
                    String discountItemCode1 = conglomDiscountRS.getString("Discount_Item_Code1");
                    long discountableAmount1 = conglomDiscountRS.getLong("Discountable_Amount1");
                    long discountAmount1 = conglomDiscountRS.getLong("Discount_Amount1");
                    String discountItemCode2 = conglomDiscountRS.getString("Discount_Item_Code1");
                    long discountableAmount2 = conglomDiscountRS.getLong("Discountable_Amount1");
                    long discountAmount2 = conglomDiscountRS.getLong("Discount_Amount1");
                    String discountItemCode3 = conglomDiscountRS.getString("Discount_Item_Code3");
                    long discountableAmount3 = conglomDiscountRS.getLong("Discountable_Amount3");
                    long discountAmount3 = conglomDiscountRS.getLong("Discount_Amount3");
                    if (discountItemCode.equals(discountItemCode1))
                    {
                      result =
                        ssDB.updateConglomDiscount1
                          (conglomCustId,
                           billedProductId,
                           billPeriodRef,
                           accountNumber,
                           netAmount,
                           vatAmount,
                           totalAmount,
                           lastUpdateId,
                           discountItemCode1,
                           discountableAmount1 + discountableAmount,
                           discountRate,
                           discountAmount1 + discountNet);
                    }
                    else if (discountItemCode.equals(discountItemCode2))
                    {
                      result =
                        ssDB.updateConglomDiscount2
                          (conglomCustId,
                           billedProductId,
                           billPeriodRef,
                           accountNumber,
                           netAmount,
                           vatAmount,
                           totalAmount,
                           lastUpdateId,
                           discountItemCode2,
                           discountableAmount2 + discountableAmount,
                           discountRate,
                           discountAmount2 + discountNet);
                    }
                    else if (discountItemCode.equals(discountItemCode3))
                    {
                      result =
                        ssDB.updateConglomDiscount3
                          (conglomCustId,
                           billedProductId,
                           billPeriodRef,
                           accountNumber,
                           netAmount,
                           vatAmount,
                           totalAmount,
                           lastUpdateId,
                           discountItemCode3,
                           discountableAmount3 + discountableAmount,
                           discountRate,
                           discountAmount3 + discountNet);
                    }
                    if (result.equals("NO MATCH"))
                    {
                      message =
                        "<font color=red> Maximum of 3 Discounts reached. Discount type "+
                        discountItemCode+" NOT APPLIED: "+result+" </font>";
                      ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
                      message = "   "+message;
                      System.out.println(message);
                      writeToLogFile(message);
                    }
                    else if (!result.equals("OK"))
                    {
                      message = "<font color=red> Problem updating Conglom_Discount : "+result+" </font>";
                      ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
                      message = "   "+message;
                      System.out.println(message);
                      writeToLogFile(message);
                      continueProcess = false;
                    }
                  }
                  else
                  {
                    // does not exist so create a new one
                    String result =
                      ssDB.insertConglomDiscount
                        (conglomCustId,
                         billedProductId,
                         billPeriodRef,
                         invoiceNumber,
                         accountNumber,
                         checkDigit,
                         discountNet,
                         discountVat,
                         discountGross,
                         lastUpdateId,
                         discountItemCode,
                         discountableAmount,
                         discountRate,
                         discountNet);
                    if (!result.equals("OK"))
                    {
                      message = "<font color=red> Problem inserting into Conglom_Discount : "+result+" </font>";
                      ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
                      message = "   "+message;
                      System.out.println(message);
                      writeToLogFile(message);
                      continueProcess = false;
                    }
                  }
                }
                catch(java.sql.SQLException ex)
                {
                  message = "Failure accessing conglomDiscountRS : "+ex.getMessage();
                  ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
                  message = "   "+message;
                  System.out.println(message);
                  writeToLogFile(message);
                  continueProcess = false;
                }
                conglomDiscountRS=null;
              }
            }
          }
        }
      }
      catch(java.sql.SQLException ex)
      {
        message = "Failure geting next view discounts : "+ex.getMessage();
        ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
        message = "   "+message;
        System.out.println(message);
        writeToLogFile(message);
        continueProcess = false;
      }
      try
      {
        viewDiscountsRS.close();
      }
      catch(java.sql.SQLException ex)
      {
        message = "Failure closing viewDiscountsRS : "+ex.getMessage();
        ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
        message = "   "+message;
        System.out.println(message);
        writeToLogFile(message);
        continueProcess = false;
      }
      viewDiscountsRS = null;
    }
    if (continueProcess)
    {
      // Create Source Invoice
      // update account totals with source invoice totals
      acctBalNetAmount = acctBalNetAmount + netAmount;
      acctBalVatAmount = acctBalVatAmount + vatAmount;
      acctBalAmount = acctBalAmount + totalDue;
      // create check net total from individual charge amounts
      long checkNetAmount =
        oneOffCharges+
        recurringCharges+
        usageCharges+
        miscCharges+
        adjustmentTotal+
        sourceDiscTotal+
        conglomDiscTotal;
      if (checkNetAmount!=acctBalNetAmount)
      {
        // create E8 exception and messages
        message =
          "Invoice net total does not reconcile with charges loaded. Invoice number ... "+invoiceNumber;
        String exceptionStatus = ss3DB.getExceptionStatus("E8");
        ssDB.writeTempException
          (conglomCustId,
           billedProductId,
           billPeriodRef,
           "Extract",
           accountNumber,
           invoiceNumber,
           billDate,
           acctBalNetAmount,
           checkNetAmount,
           exceptionStatus,
           "E8",
           lastUpdateId);
        message =
          "<font color=red>"+message+"</font>";
        ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
        message = "   "+message;
        System.out.println(message);
        writeToLogFile(message);
      }
      String result = ssDB.insertSourceInvoice
        (conglomCustId,
         billedProductId,
         billPeriodRef,
         invoiceNumber,
         accountNumber,
         checkDigit,
         billDate,
         sourceConglomId,
         BPSD,
         BPED,
         "OPEN",
         netAmount,
         vatAmount,
         totalDue,
         acctBalNetAmount,
         acctBalVatAmount,
         acctBalAmount,
         oneOffCharges,
         recurringCharges,
         usageCharges,
         miscCharges,
         adjustmentTotal,
         sourceDiscTotal,
         conglomDiscTotal,
         installCharges,
         rentalCharges,
         callinkCharges,
         vpnCharges,
         callCharges,
         authcodeCharges,
         easyUsageCharges,
         easyQuarterlyCharges,
         sundryCharges,
         specialCharges,
         creditsTotal,
         debitsTotal,
         lastUpdateId);
      if (!result.equals("OK"))
      {
        message = "<font color=red> Problem inserting into Source_Invoice : "+result+" </font>";
        ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
        message = "   "+message;
        System.out.println(message);
        writeToLogFile(message);
        continueProcess = false;
      }
    }
    return continueProcess;
  }

  // process any local data
  private boolean processLocalData()
  {
    boolean continueProcess = true;
    message = "Checking local data configuration";
    ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
    message = "   "+message;
    System.out.println(message);
    writeToLogFile(message);
    // get local configurations for the account/product
    ResultSet custLocalDataRefItemRS = ss3DB.getLocalDataDataItems(conglomCustId,billedProductId);
    String currentReportType = "XXXXXX";
    boolean hasLocalData = false;
    try
    {
      while(custLocalDataRefItemRS.next())
      {
        String reportType = custLocalDataRefItemRS.getString("Report_Type");
        String ldRefType = custLocalDataRefItemRS.getString("LD_Ref_Type");
        int ldColumnSeqNo = custLocalDataRefItemRS.getInt("LD_Column_Seq_No");
        int ldSortSeqNo = custLocalDataRefItemRS.getInt("LD_Sort_Seq_No");
        hasLocalData = true;
        // issue message on first configuration for report type
        if (!currentReportType.equals(reportType))
        {
          currentReportType = reportType;
          if (reportType.equals("CONSOL"))
            message = "Processing consolidated local data";
          else
            message = "Processing billing summary local data";
            ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
            message = "   "+message;
            System.out.println(message);
            writeToLogFile(message);
        }
        // connect to Oracle
        if (openOracleConnection(2))
          o2DB = new oracleDB(oracleConn2);
        else
          continueProcess = false;
        if (continueProcess)
        {
          // get local data configuration data from Goldfish
          ResultSet refItemsRS = null;
          boolean hasRefItems = false;
          if (reportType.equals("CONSOL"))
            refItemsRS=o2DB.getConsolRefItems(sourceConglomId,sourceSystemId,ldRefType);
          else
            refItemsRS=o2DB.getBillsumRefItems(sourceConglomId,ldRefType);
          try
          {
            while(refItemsRS.next())
            {
              hasRefItems = true;
              String accountNumber = refItemsRS.getString("Account_Number");
              String description = refItemsRS.getString("Description")+".";
              String referenceItem = refItemsRS.getString("Reference_Item");
              String billingNumber = refItemsRS.getString("Billing_Number");
              String referenceType = refItemsRS.getString("Reference_Type");
              String referenceItemSort = "NULL";
              if (ldSortSeqNo!=0)
                referenceItemSort = referenceItem;
              // if column seq no is 1 check if local data item exists
              // and create it if it does not exist, otherwise update it
              // if column seq no is not 1 then update it
              if (ldColumnSeqNo==1)
              {
                if (!ss3DB.localDataExists
                    (conglomCustId,
                     billedProductId,
                     reportType,
                     accountNumber,
                     billingNumber))
                  {
                    // create new source account local data populating just first set of columns
                    if (!ssDB.insertLocalData
                          (conglomCustId,
                           billedProductId,
                           reportType,
                           accountNumber,
                           billingNumber,
                           referenceItem,
                           referenceItemSort,
                           lastUpdateId))
                    {
                      message = "Failure creating new source acct data item";
                      ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
                      message = "   "+message;
                      System.out.println(message);
                      writeToLogFile(message);
                      continueProcess = false;
                    }
                  }
                  else
                  {
                  // update existing source account local data first set of columns
                  if (!ssDB.updateLocalData
                        (conglomCustId,
                         billedProductId,
                         reportType,
                         accountNumber,
                         billingNumber,
                         referenceItem,
                         referenceItemSort,
                         ldColumnSeqNo,
                         lastUpdateId))
                  {
                    message = "Failure updating new source acct data item";
                    ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
                    message = "   "+message;
                    System.out.println(message);
                    writeToLogFile(message);
                    continueProcess = false;
                  }
                }
              }
              else
              {
                // update existing source account local data
                if (!ssDB.updateLocalData
                      (conglomCustId,
                       billedProductId,
                       reportType,
                       accountNumber,
                       billingNumber,
                       referenceItem,
                       referenceItemSort,
                       ldColumnSeqNo,
                       lastUpdateId))
                {
                  message = "Failure updating new source acct data item";
                  ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
                  message = "   "+message;
                  System.out.println(message);
                  writeToLogFile(message);
                  continueProcess = false;
                }
              }
            }
          }
          catch(java.sql.SQLException ex)
          {
            message = "Failure accessing refItemsRS : "+ex.getMessage();
            ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
            message = "   "+message;
            System.out.println(message);
            writeToLogFile(message);
            continueProcess = false;
          }
          try
          {
            refItemsRS.close();
            refItemsRS=null;
          }
          catch(java.sql.SQLException ex)
          {
            message = "Failure closing refItemsRS : "+ex.getMessage();
            ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
            message = "   "+message;
            System.out.println(message);
            writeToLogFile(message);
            continueProcess = false;
          }
          if (!hasRefItems)
          {
            message = "No local data reference items found on Goldfish for "+reportType+"/"+ldRefType;
            ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
            message = "   "+message;
            System.out.println(message);
            writeToLogFile(message);
          }
          refItemsRS=null;
        }
        closeOracleConnection(oracleConn2);
        oracleConn2 = null;
        o2DB=null;
      }
    }
    catch(java.sql.SQLException ex)
    {
      message = "Failure processing Cust_Local_Data_Ref_Item : "+ex.getMessage();
      ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
      message = "   "+message;
      System.out.println(message);
      writeToLogFile(message);
      continueProcess = false;

    }
    if (hasLocalData)
      message = "Finished loading of local data";
    else
      message = "No local data configuration for this product";
    ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
    message = "   "+message;
    System.out.println(message);
    writeToLogFile(message);
    return continueProcess;
  }

  // for quarterly PCB bills process prior period bills not already loaded
  private boolean processPriorPeriodTBills()
  {
    boolean continueProcess = true;
    // connect to oracle
    if (openOracleConnection(1))
      oDB = new oracleDB(oracleConn);
    else
      continueProcess = false;
    // get prior tbills, check not already billed and process charges
    tBillsRS = oDB.getPriorPCBTBills(sourceConglomId,BPSD);
    // load prior period t_bills recordset into file
    tBillsFileName = workDir + File.separator + "priorPeriodTBills.txt";
    tBillsFileCreated = createTBillsFile(tBillsRS,tBillsFileName);
    tBillsRS = null;
    closeOracleConnection(oracleConn);
    oracleConn = null;
    oDB = null;
    if (openTBillsFile(tBillsFileName))
    {
      tBillsFileLine = nextTBillsFileLine();
      while (((!tBillsFileLine.equals("EOF"))&&(continueProcess)&&(!tBillsFileLine.startsWith("IO"))))
      {
        decodeTBillsLine(tBillsFileLine);
        // initialise source invoice totals
        initSourceInvoiceTotals();
        // ignore prior period bill if already present
        if(!ssDB.sourceInvoiceExists(conglomCustId,billedProductId,invoiceNumber))
        {
          message = "Start of extract for Source Invoice No: "+invoiceNumber+" (prior period)";
          ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
          message = "   "+message;
          System.out.println(message);
          writeToLogFile(message);
           // Install Charges
          if (openOracleConnection(2))
            o2DB = new oracleDB(oracleConn2);
          else
            continueProcess = false;
          if (continueProcess)
          {
            installChargesRS = o2DB.getPriorPCBInstallCharges(sourceConglomId,invoiceNumber,accountNumber);
            try
            {
              while(installChargesRS.next())
              {
                // store details for T_Install_Charges
                long installationCharge = installChargesRS.getLong("Installation_Charge");
                String installationDate = installChargesRS.getString("Installation_Date");
                String equipmentCode = installChargesRS.getString("Equipment_Code");
                long equipmentQuantity = installChargesRS.getLong("Equipment_Quantity");
                String equipmentCodeDescription = installChargesRS.getString("Equipment_Code_Description");
                String circuitReference = installChargesRS.getString("Circuit_Reference");
                String contractNumber = installChargesRS.getString("Contract_Number");
                String chargeType = "PINS";
                // increment running totals
                installCharges = installCharges + installationCharge;
                oneOffCharges = oneOffCharges + installationCharge;
                // insert one_off_charge
                String result =
                  ssDB.insertOneOffCharge
                  (conglomCustId,
                   billedProductId,
                   billPeriodRef,
                   invoiceNumber,
                   accountNumber,
                   chargeType,
                   installationDate,
                   contractNumber,
                   circuitReference,
                   equipmentCodeDescription,
                   equipmentQuantity,
                   installationCharge,
                   lastUpdateId);
                if (!result.equals("OK"))
                {
                  message = "<font color=red> Problem inserting into Source_One_Off_Charge (prior period): "+result+" </font>";
                  ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
                  message = "   "+message;
                  System.out.println(message);
                  writeToLogFile(message);
                  continueProcess = false;
                }
              }
            }
            catch (java.sql.SQLException ex)
            {
              message = "Failure geting next installChargesRS (prior period): "+ex.getMessage();
              ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
              message = "   "+message;
              System.out.println(message);
              writeToLogFile(message);
              continueProcess = false;
            }
            try
            {
              installChargesRS.close();
              installChargesRS = null;
            }
            catch (java.sql.SQLException ex)
            {
              message = "Failure closing installChargesRS (prior period): "+ex.getMessage();
              ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
              message = "   "+message;
              System.out.println(message);
              writeToLogFile(message);
              continueProcess = false;
            }
          }
          installChargesRS = null;
          closeOracleConnection(oracleConn2);
          oracleConn2=null;
          o2DB = null;
          // Rental Charges
          if (continueProcess)
            if (openOracleConnection(2))
              o2DB = new oracleDB(oracleConn2);
            else
              continueProcess = false;
          if (continueProcess)
          {
            // Rental Charges
            rentalChargesRS = o2DB.getPriorPCBRentalCharges(sourceConglomId,invoiceNumber,accountNumber);
            try
            {
              while(rentalChargesRS.next())
              {
                // store details for T_Equipment_Charges\T_Previous Charges
                long equipmentRentalAmount = rentalChargesRS.getLong("Equipment_Rental_Amount");
                String equipmentStartDate = rentalChargesRS.getString("Equipment_Start_Date");
                String equipmentStopDate = rentalChargesRS.getString("Equipment_Stop_Date");
                long equipmentQuantity = rentalChargesRS.getLong("Equipment_Quantity");
                String equipmentCodeDescription = rentalChargesRS.getString("Equipment_Code_Description");
                long vatAmount = rentalChargesRS.getLong("VAT_Amount");
                long totalAmount = rentalChargesRS.getLong("Total_Amount");
                // update vat amount
                if (vatExemptInd.equals("Y"))
                  vatAmount = 0;
                else
                  vatAmount = (equipmentRentalAmount * vatRate)/100;
                String aEndAddress = rentalChargesRS.getString("A_End_Address");
                String bEndAddress = rentalChargesRS.getString("B_End_Address");
                String contractNumber = rentalChargesRS.getString("Contract_Number");
                String circuitLength = rentalChargesRS.getString("Circuit_Length");
                String circuitReference = rentalChargesRS.getString("Circuit_Reference");
                String chargeType = "PREN";
                // increment running totals
                rentalCharges = rentalCharges + equipmentRentalAmount;
                recurringCharges = recurringCharges + equipmentRentalAmount;
                // insert into sourceRecurringCharge
                String result =
                  ssDB.insertRecurringCharge(
                    conglomCustId,
                    billedProductId,
                    billPeriodRef,
                    invoiceNumber,
                    accountNumber,
                    chargeType,
                    equipmentStartDate,
                    equipmentStopDate,
                    equipmentCodeDescription,
                    equipmentQuantity,
                    equipmentRentalAmount,
                    vatAmount,
                    lastUpdateId,
                    contractNumber,
                    circuitReference,
                    aEndAddress,
                    bEndAddress,
                    circuitLength,
                    vatExemptInd);
                if (!result.equals("OK"))
                {
                  message = "<font color=red> Problem inserting into Source_Recurring_Charge (prior period): "+result+" </font>";
                  ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
                  message = "   "+message;
                  System.out.println(message);
                  writeToLogFile(message);
                  continueProcess = false;
                }
              }
            }
            catch (java.sql.SQLException ex)
            {
              message = "Failure geting next rentalChargesRS (prior period): "+ex.getMessage();
              ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
              message = "   "+message;
              System.out.println(message);
              writeToLogFile(message);
              continueProcess = false;
            }
            try
            {
              rentalChargesRS.close();
              rentalChargesRS = null;
            }
            catch (java.sql.SQLException ex)
            {
              message = "Failure closing rentalChargesRS (prior period): "+ex.getMessage();
              ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
              message = "   "+message;
              System.out.println(message);
              writeToLogFile(message);
              continueProcess = false;
            }
          }
          rentalChargesRS = null;
          closeOracleConnection(oracleConn2);
          oracleConn2=null;
          o2DB = null;
          // Sundry charges
          if (continueProcess)
          {
            if (openOracleConnection(2))
              o2DB = new oracleDB(oracleConn2);
            else
              continueProcess = false;
          }
          if (continueProcess)
          {
            sundryChargesRS=o2DB.getPriorPCBSundryCharges(sourceConglomId,invoiceNumber,accountNumber);
            try
            {
              while (sundryChargesRS.next())
              {
                // get sundry charge details
                long chargeAmount = sundryChargesRS.getLong("Charge_Amount");
                String sundryCode = sundryChargesRS.getString("Sundry_Code");
                String sundryDescription = sundryChargesRS.getString("Sundry_Description");
                String chargeType = "PSUN";
                // increment totals
                sundryCharges = sundryCharges + chargeAmount;
                miscCharges = miscCharges + chargeAmount;
                // insert into source sundry charge
                String result = ssDB.insertSundryCharge
                  (conglomCustId,
                   billedProductId,
                   billPeriodRef,
                   invoiceNumber,
                   accountNumber,
                   chargeType,
                   sundryCode,
                   sundryDescription,
                   chargeAmount,
                   lastUpdateId);
                if (!result.equals("OK"))
                {
                   message = "<font color=red> Problem inserting into Source_Sundry_Charge (prior period): "+result+" </font>";
                   ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
                   message = "   "+message;
                   System.out.println(message);
                   writeToLogFile(message);
                   continueProcess = false;
                }
              }
              try
              {
                sundryChargesRS.close();
                sundryChargesRS = null;
              }
              catch (java.sql.SQLException ex)
              {
                message = "Failure closing sundryChargesRS (prior period): "+ex.getMessage();
                ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
                message = "   "+message;
                System.out.println(message);
                writeToLogFile(message);
                continueProcess = false;
              }
            }
            catch (java.sql.SQLException ex)
            {
              message = "Failure geting next sundryChargesRS (prior period): "+ex.getMessage();
              ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
              message = "   "+message;
              System.out.println(message);
              writeToLogFile(message);
              continueProcess = false;
            }
          }
          sundryChargesRS=null;
          closeOracleConnection(oracleConn2);
          oracleConn2=null;
          o2DB=null;
          // Create Source Invoice
          if (continueProcess)
          {
            // update account totals with source invoice totals
            acctBalNetAmount = acctBalNetAmount + netAmount;
            acctBalVatAmount = acctBalVatAmount + vatAmount;
            acctBalAmount = acctBalAmount + totalDue;
            // create check net total from individual charge amounts
            long checkNetAmount =
              oneOffCharges+
              recurringCharges+
              miscCharges;
            if (checkNetAmount!=acctBalNetAmount)
            {
              // create E8 exception and messages
              message =
                "Invoice net total does not reconcile with charges loaded. Invoice number ... "+invoiceNumber;
              String exceptionStatus = ss3DB.getExceptionStatus("E8");
              ssDB.writeTempException
                (conglomCustId,
                 billedProductId,
                 billPeriodRef,
                 "Extract",
                 accountNumber,
                 invoiceNumber,
                 billDate,
                 acctBalNetAmount,
                 checkNetAmount,
                 exceptionStatus,
                 "E8",
                 lastUpdateId);
              message =
                "<font color=red>"+message+"</font>";
              ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
              message = "   "+message;
              System.out.println(message);
              writeToLogFile(message);
            }
            String result = ssDB.insertSourceInvoice
              (conglomCustId,
               billedProductId,
               billPeriodRef,
               invoiceNumber,
               accountNumber,
               checkDigit,
               billDate,
               sourceConglomId,
               BPSD,
               BPED,
               "OPEN",
               netAmount,
               vatAmount,
               totalDue,
               acctBalNetAmount,
               acctBalVatAmount,
               acctBalAmount,
               oneOffCharges,
               recurringCharges,
               usageCharges,
               miscCharges,
               adjustmentTotal,
               sourceDiscTotal,
               conglomDiscTotal,
               installCharges,
               rentalCharges,
               callinkCharges,
               vpnCharges,
               callCharges,
               authcodeCharges,
               easyUsageCharges,
               easyQuarterlyCharges,
               sundryCharges,
               specialCharges,
               creditsTotal,
               debitsTotal,
               lastUpdateId);
            if (!result.equals("OK"))
            {
              message = "<font color=red> Problem inserting into Source_Invoice (prior period): "+result+" </font>";
              ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
              message = "   "+message;
              System.out.println(message);
              writeToLogFile(message);
              continueProcess = false;
            }
          }
        }
        tBillsFileLine = nextTBillsFileLine();
      }
    }
    closeTBillsFile();
    return continueProcess;
  }

  // process PCB Credits creating source adjustments and where required source invoices
  private boolean processPCBCredits()
  {
    boolean continueProcess = true;
    message = "Processing PCB Credits";
    ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
    message = "   "+message;
    System.out.println(message);
    writeToLogFile(message);
    // clear existing PCB credits (have to do this here when there are no source invoices on the bill)
    ssDB.clearPCBCredits(conglomCustId,billedProductId,billPeriodRef);
    // connect to Oracle
    if (openOracleConnection(2))
      o2DB = new oracleDB(oracleConn2);
    else
      continueProcess = false;
    // process PCB credits
    if (continueProcess)
    {
      creditNotesRS = o2DB.getPCBCreditNotes(sourceConglomId,BPSD,BPED);
      // load pcb credits recordset into file
      pcbCreditsFileName = workDir + File.separator + "pcbCredits.txt";
      pcbCreditsFileCreated = createPCBCreditsFile(creditNotesRS,pcbCreditsFileName);
      creditNotesRS = null;
      closeOracleConnection(oracleConn2);
      oracleConn2 = null;
      o2DB = null;
      if (openPCBCreditsFile(pcbCreditsFileName))
      {
        pcbCreditsFileLine = nextPCBCreditsFileLine();
        while (((!pcbCreditsFileLine.equals("EOF"))&&(continueProcess)&&(!pcbCreditsFileLine.startsWith("IO"))))
        {
          decodePCBCreditsLine(pcbCreditsFileLine);
          boolean alreadyDone = false;
          String creditStatus = "OPEN";
          // check for a matching Conglom Discount
          ResultSet conglomDiscountRS = ss3DB.getConglomDiscount(billedProductId,accountNumber,creditNoteNumber);
          try
          {
            if (conglomDiscountRS.next())
            {
              // match
              long exceptionAmount2 = conglomDiscountRS.getLong("Net_Amount");
              long discountAdjCreditAmount  = conglomDiscountRS.getLong("Total_Amount");
              creditStatus = "DUPLICATE";
              if (discountAdjCreditAmount==grossAmount)
              {
                // create E2 exception and messages
                message =
                  "E2. Docket Number ... "+creditNoteNumber;
                String exceptionStatus = ss3DB.getExceptionStatus("E2");
                ssDB.writeTempException
                  (conglomCustId,
                   billedProductId,
                   billPeriodRef,
                   "Extract",
                   accountNumber,
                   creditNoteNumber,
                   billDate,
                   netAmount,
                   exceptionAmount2,
                   exceptionStatus,
                   "E2",
                   lastUpdateId);
                message =
                  "<font color=red>"+message+"</font>";
                ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
                message = "   "+message;
                System.out.println(message);
                writeToLogFile(message);
              }
            }
            else
            {
              // check for matching conglom adjustment
              ResultSet conglomAdjustmentRS = ss3DB.getConglomAdjustment(billedProductId,accountNumber,creditNoteNumber);
              try
              {
                if (conglomAdjustmentRS.next())
                {
                  // match
                  long exceptionAmount2 = conglomAdjustmentRS.getLong("Net_Amount");
                  long discountAdjCreditAmount  = conglomAdjustmentRS.getLong("Total_Amount");
                  creditStatus = "DUPLICATE";
                  if (discountAdjCreditAmount==grossAmount)
                  {
                    // create E2 exception and messages
                    message =
                      "E2. Docket Number ... "+creditNoteNumber;
                    String exceptionStatus = ss3DB.getExceptionStatus("E2");
                    ssDB.writeTempException
                      (conglomCustId,
                       billedProductId,
                       billPeriodRef,
                       "Extract",
                       accountNumber,
                       creditNoteNumber,
                       billDate,
                       netAmount,
                       exceptionAmount2,
                       exceptionStatus,
                       "E2",
                       lastUpdateId);
                    message =
                      "<font color=red>"+message+"</font>";
                    ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
                    message = "   "+message;
                    System.out.println(message);
                    writeToLogFile(message);
                  }
                }
              }
              catch(java.sql.SQLException ex)
              {
                message = "Failure accessing Conglom_Adjustment (PCB Credits) : "+ex.getMessage();
                ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
                message = "   "+message;
                System.out.println(message);
                writeToLogFile(message);
                continueProcess = false;
              }
              conglomAdjustmentRS=null;
            }
          }
          catch(java.sql.SQLException ex)
          {
            message = "Failure accessing Conglom_Discount (PCB Credits) : "+ex.getMessage();
            ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
            message = "   "+message;
            System.out.println(message);
            writeToLogFile(message);
            continueProcess = false;
          }
          conglomDiscountRS=null;
          // check if source adjustment has already been created in previous run
          if (ss3DB.sourceAdjustmentExists(conglomCustId,billedProductId,creditNoteNumber))
          {
            message = "Ignoring PCB Credit "+creditNoteNumber+" as it has already been loaded for this customer";
            ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
            message = "   "+message;
            System.out.println(message);
            writeToLogFile(message);
            alreadyDone = true;
          }
          if ((continueProcess)&&(!alreadyDone))
          {
            // check for matching source invoice
            String sourceInvoiceNo =
              ss3DB.getSourceInvoiceNo(conglomCustId,billedProductId,billPeriodRef,accountNumber);
            if (sourceInvoiceNo==creditNoteNumber)
            {
              // match so update with credit amounts
              if (!ssDB.updateSourceInvoiceForPCBCredit
                    (conglomCustId,billedProductId,billPeriodRef,accountNumber,creditNoteNumber,
                     netAmount,vatAmount,grossAmount))
              {
                message = "Failure updating source invoice for "+creditNoteNumber+" for PCB credit";
                ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
                message = "   "+message;
                System.out.println(message);
                writeToLogFile(message);
                continueProcess = false;
              }
            }
            else
            {
              // no match so create source invoice
              String result =
                ssDB.insertSourceInvoice
                  (conglomCustId,
                   billedProductId,
                   billPeriodRef,
                   creditNoteNumber,
                   accountNumber,
                   checkDigit,
                   creditNoteDate,
                   sourceConglomId,
                   BPSD,
                   BPED,
                   "OPEN",
                   0,
                   0,
                   0,
                   netAmount,
                   vatAmount,
                   grossAmount,
                   0,
                   0,
                   0,
                   0,
                   netAmount,
                   0,
                   0,
                   0,
                   0,
                   0,
                   0,
                   0,
                   0,
                   0,
                   0,
                   0,
                   0,
                   netAmount,
                   0,
                   lastUpdateId);
              if (!result.equals("OK"))
              {
                message = "Failure creating source invoice for "+creditNoteNumber+" for PCB Credit :"+result;
                ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
                message = "   "+message;
                System.out.println(message);
                writeToLogFile(message);
                continueProcess = false;
              }
            }
          }
          if ((continueProcess)&&(!alreadyDone))
          {
            // insert source adjustment
            String result =
              ssDB.insertPCBSourceAdjustment
                (conglomCustId,
                 billedProductId,
                 billPeriodRef,
                 accountNumber,
                 checkDigit,
                 creditStatus,
                 creditNoteNumber,
                 creditNoteDate,
                 netAmount,
                 vatAmount,
                 grossAmount,
                 lastUpdateId);
            if (!result.endsWith("OK"))
            {
              // raise exception if duplicate
              message = "Failure inserting source adjustment for PCB Credit : "+result;
              ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
              message = "   "+message;
              System.out.println(message);
              writeToLogFile(message);
              continueProcess = false;
            }
            else
            {
              message = "Inserting PCB Credit "+creditNoteNumber;
              ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
              message = "   "+message;
              System.out.println(message);
              writeToLogFile(message);
            }
          }
          pcbCreditsFileLine = nextPCBCreditsFileLine();
        }
      }
      closePCBCreditsFile();
    }
    return continueProcess;
  }

  // apply lead account discounts
  private boolean processLeadAccountDiscounts()
  {
    boolean continueProcess = true;
    viewDiscountsRS=ss3DB.getViewDiscounts(conglomCustId,billedProductId,mthlyBillStartDate);
    try
    {
      while(viewDiscountsRS.next())
      {
        String discountItemCode = viewDiscountsRS.getString("Discount_Item_Code");
        long wholeValuePercent = viewDiscountsRS.getLong("Whole_Value_Percent");
        long discountRate = viewDiscountsRS.getLong("Discount_Rate");
        String leadAccountId = viewDiscountsRS.getString("Lead_Account_Id");
        String leadAccountCheckDigit = viewDiscountsRS.getString("Lead_Acct_Check_Digit");
        String acctsExcludedInd = viewDiscountsRS.getString("Accts_Excluded_Ind");
        String discountTypeDesc = viewDiscountsRS.getString("Discount_Type_Decs");
        String shortDescription = viewDiscountsRS.getString("Short_Description");
        int seqNo = viewDiscountsRS.getInt("Seq_No");
        if ((!leadAccountId.equals("NULL")))
        {
          long discountableAmount = 0;
          if (shortDescription.equals("Installs"))
            discountableAmount = laInstallCharges;
          else if (shortDescription.equals("Rentals"))
            discountableAmount = laRentalCharges;
          else if (shortDescription.equals("All Usage"))
            discountableAmount = laUsageCharges;
          else if (shortDescription.equals("Callink"))
            discountableAmount = laCallinkCharges;
          else if (shortDescription.equals("VPN"))
            discountableAmount = laVPNCharges;
          else if (shortDescription.equals("Call"))
            discountableAmount = laCallCharges;
          else if (shortDescription.equals("Authcode"))
            discountableAmount = laAuthcodeCharges;
          else if (shortDescription.equals("Sundry"))
            discountableAmount = laSundryCharges;
          else if (shortDescription.equals("Special"))
            discountableAmount = laSpecialCharges;
          else if (shortDescription.equals("Easy Acc Usage"))
            discountableAmount = laEasyUsageCharges;
          // proceed if discountable amount is not zero
          if (discountableAmount!=0)
          {
            long discountNet = ( discountableAmount * wholeValuePercent * discountRate * -1 ) / 100000000;
            long discountVat = 0;
            if (!vatExemptInd.equals("Y"))
              discountVat = ( discountNet * vatRate ) / 1000;
            long discountGross = discountNet + discountVat;
            // increment totals
            laConglomDiscount = laConglomDiscount + discountNet;
            laBalNetAmount = laBalNetAmount + discountNet;
            laBalVatAmount = laBalVatAmount + discountVat;
            laBalAmount = laBalAmount + discountGross;
            // check for existing conglom discount
            ResultSet conglomDiscountRS =
              ss3DB.getConglomDiscount
                (conglomCustId,billedProductId,billPeriodRef,leadAccountId);
            try
            {
              if (conglomDiscountRS.next())
              {
                String result = "NO MATCH";
                // does exist so update
                long netAmount = conglomDiscountRS.getLong("Net_Amount") + discountNet;
                long vatAmount = conglomDiscountRS.getLong("VAT Amount") + discountVat;
                long totalAmount = conglomDiscountRS.getLong("Total_Amount") +discountGross;
                String discountItemCode1 = conglomDiscountRS.getString("Discount_Item_Code1");
                long discountableAmount1 = conglomDiscountRS.getLong("Discountable_Amount1");
                long discountAmount1 = conglomDiscountRS.getLong("Discount_Amount1");
                String discountItemCode2 = conglomDiscountRS.getString("Discount_Item_Code1");
                long discountableAmount2 = conglomDiscountRS.getLong("Discountable_Amount1");
                long discountAmount2 = conglomDiscountRS.getLong("Discount_Amount1");
                String discountItemCode3 = conglomDiscountRS.getString("Discount_Item_Code3");
                long discountableAmount3 = conglomDiscountRS.getLong("Discountable_Amount3");
                long discountAmount3 = conglomDiscountRS.getLong("Discount_Amount3");
                if (discountItemCode.equals(discountItemCode1))
                {
                  result =
                    ssDB.updateConglomDiscount1
                      (conglomCustId,
                       billedProductId,
                       billPeriodRef,
                       leadAccountId,
                       netAmount,
                       vatAmount,
                       totalAmount,
                       lastUpdateId,
                       discountItemCode1,
                       discountableAmount1 + discountableAmount,
                       discountRate,
                       discountAmount1 + discountNet);
                }
                else if (discountItemCode.equals(discountItemCode2))
                {
                  result =
                    ssDB.updateConglomDiscount2
                      (conglomCustId,
                       billedProductId,
                       billPeriodRef,
                       leadAccountId,
                       netAmount,
                       vatAmount,
                       totalAmount,
                       lastUpdateId,
                       discountItemCode2,
                       discountableAmount2 + discountableAmount,
                       discountRate,
                       discountAmount2 + discountNet);
                }
                else if (discountItemCode.equals(discountItemCode3))
                {
                  result =
                    ssDB.updateConglomDiscount3
                      (conglomCustId,
                       billedProductId,
                       billPeriodRef,
                       leadAccountId,
                       netAmount,
                       vatAmount,
                       totalAmount,
                       lastUpdateId,
                       discountItemCode3,
                       discountableAmount3 + discountableAmount,
                       discountRate,
                       discountAmount3 + discountNet);
                }
                if (result.equals("NO MATCH"))
                {
                  message =
                    "<font color=red> Maximum of 3 Discounts reached. Discount type "+
                    discountItemCode+" NOT APPLIED (lead account) : "+result+" </font>";
                  ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
                  message = "   "+message;
                  System.out.println(message);
                  writeToLogFile(message);
                }
                else if (!result.equals("OK"))
                {
                  message = "<font color=red> Problem updating Conglom_Discount (lead account) : "+result+" </font>";
                  ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
                  message = "   "+message;
                  System.out.println(message);
                  writeToLogFile(message);
                  continueProcess = false;
                }
              }
              else
              {
                // does not exist so create a new one
                String laSourceInvoiceNo =
                  ssDB.getSourceInvoiceNo(conglomCustId,billedProductId,billPeriodRef,leadAccountId);
                String result =
                  ssDB.insertConglomDiscount
                    (conglomCustId,
                     billedProductId,
                     billPeriodRef,
                     laSourceInvoiceNo,
                     leadAccountId,
                     checkDigit,
                     discountNet,
                     discountVat,
                     discountGross,
                     lastUpdateId,
                     discountItemCode,
                     discountableAmount,
                     discountRate,
                     discountNet);
                if (!result.equals("OK"))
                {
                  message = "<font color=red> Problem inserting into Conglom_Discount (lead account) : "+result+" </font>";
                  ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
                  message = "   "+message;
                  System.out.println(message);
                  writeToLogFile(message);
                  continueProcess = false;
                }
              }
            }
            catch(java.sql.SQLException ex)
            {
              message = "Failure accessing conglomDiscountRS (lead account) : "+ex.getMessage();
              ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
              message = "   "+message;
              System.out.println(message);
              writeToLogFile(message);
              continueProcess = false;
            }
            conglomDiscountRS=null;
            // update source invoice for lead account
            sourceInvoiceRS = ss3DB.getLaSourceInvoice(conglomCustId,billedProductId,billPeriodRef,leadAccountId);
            try
            {
              if (sourceInvoiceRS.next())
              {
                long acctBalNetAmount = sourceInvoiceRS.getLong("Acct_Bal_Net_Amount")+laBalNetAmount;
                long acctBalVattAmount = sourceInvoiceRS.getLong("Acct_Bal_Vat_Amount")+laBalVatAmount;
                long acctBalAmount = sourceInvoiceRS.getLong("Acct_Bal_Amount")+laBalAmount;
                long conglomDiscTotal = sourceInvoiceRS.getLong("Conglom_Discount_Total")+laConglomDiscount;
                if ( ssDB.updateLaSourceInvoice
                      (conglomCustId,
                       billedProductId,
                       billPeriodRef,
                       leadAccountId,
                       acctBalNetAmount,
                       acctBalVatAmount,
                       acctBalAmount,
                       conglomDiscTotal))
                {
                  message = "Failed to update source invoice for lead account "+leadAccountId;
                  ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
                  message = "   "+message;
                  System.out.println(message);
                  writeToLogFile(message);
                  continueProcess = false;
                }
              }
              else
              {
                message = "Missing source invoice for lead account "+leadAccountId;
                ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
                message = "   "+message;
                System.out.println(message);
                writeToLogFile(message);
                continueProcess = false;
              }
            }
            catch(java.sql.SQLException ex)
            {
              message = "Failure geting source invoice for lead account "+leadAccountId+" : "+ex.getMessage();
              ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
              message = "   "+message;
              System.out.println(message);
              writeToLogFile(message);
              continueProcess = false;
            }
          }
        }
      }
    }
    catch(java.sql.SQLException ex)
    {
      message = "Failure geting next view discounts (lead account) : "+ex.getMessage();
      ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
      message = "   "+message;
      System.out.println(message);
      writeToLogFile(message);
      continueProcess = false;
    }
    try
    {
      viewDiscountsRS.close();
    }
    catch(java.sql.SQLException ex)
    {
      message = "Failure closing viewDiscountsRS (lead account) : "+ex.getMessage();
      ss2DB.writeLogMessage(conglomCustId,BPSD,lastUpdateId,timestamp(),"I",message);
      message = "   "+message;
      System.out.println(message);
      writeToLogFile(message);
      continueProcess = false;
    }
    viewDiscountsRS = null;
    return continueProcess;
  }

  // set all source invoice totals to zero
  private void initSourceInvoiceTotals()
  {
    authcodeCharges = 0;
    billNumberDiscount = 0;
    billNumberTotal = 0;
    callCharges = 0;
    callinkCharges = 0;
    easyQuarterlyCharges = 0;
    easyQuarterlyChargesSum = 0;
    easyUsageCharges = 0;
    easyUsageChargesSum = 0;
    installCharges = 0;
    miscCharges = 0;
    oneOffCharges = 0;
    recurringCharges = 0;
    rentalCharges = 0;
    sourceDiscTotal = 0;
    specialCharges = 0;
    sundryCharges = 0;
    usageCharges = 0;
    vpnCharges = 0;
    acctBalAmount = 0;
    acctBalNetAmount = 0;
    acctBalVatAmount = 0;
    adjustmentTotal = 0;
    creditsTotal = 0;
    debitsTotal = 0;
    conglomDiscTotal = 0;
  }

  // initialise all lead account totals to zero
  private void initLeadAccountTotals()
  {
    laBalAmount = 0;
    laBalNetAmount = 0;
    laBalVatAmount = 0;
    laAuthcodeCharges = 0;
    laCallCharges = 0;
    laCallinkCharges = 0;
    laEasyUsageCharges = 0;
    laInstallCharges = 0;
    laRentalCharges = 0;
    laSpecialCharges = 0;
    laSundryCharges = 0;
    laUsageCharges = 0;
    laVPNCharges = 0;
    laConglomDiscount = 0;
    laDiscountableAmount = 0;
    laDiscountToApplyNet = 0;
    laDiscountToApplyGross = 0;
    laDiscountToApplyVat = 0;
  }

  // open tbills work file
  private boolean openTBillsFile(String tbFilename)
  {
    boolean result = false;
    try
    {
      tbBR = new BufferedReader(new FileReader(tbFilename));
      result = true;
    }
    catch (IOException e)
    {
      System.out.println("   Error opening tbills work file : "+e.toString());
    }
    return result;
  }

  // close tbills work file
  private String nextTBillsFileLine()
  {
    String fLine = "";
    try
    {
      fLine = tbBR.readLine();
      if (fLine==null)
        fLine = "EOF";
    }
    catch (IOException e)
    {
      fLine = "IO Error: "+e.toString();
    }
    return fLine;
  }

  // close tbills work file
  private void closeTBillsFile()
  {
    try
    {
      tbBR.close();
    }
    catch (IOException e)
    {
      System.out.println("   Error closing tbills work file : "+e.toString());
    }
  }

  // open pcb credits work file
  private boolean openPCBCreditsFile(String pcFilename)
  {
    boolean result = false;
    try
    {
      pcBR = new BufferedReader(new FileReader(pcFilename));
      result = true;
    }
    catch (IOException e)
    {
      System.out.println("   Error opening pcb credits work file : "+e.toString());
    }
    return result;
  }

  // close pcb credits work file
  private String nextPCBCreditsFileLine()
  {
    String fLine = "";
    try
    {
      fLine = pcBR.readLine();
      if (fLine==null)
        fLine = "EOF";
    }
    catch (IOException e)
    {
      fLine = "IO Error: "+e.toString();
    }
    return fLine;
  }

  // close pcb credits work file
  private void closePCBCreditsFile()
  {
    try
    {
      pcBR.close();
    }
    catch (IOException e)
    {
      System.out.println("   Error closing pcb credits work file : "+e.toString());
    }
  }

  // open log file
  private void openLogFile()
  {
    logBW = null;
    String logDate = new java.util.Date().toString();
    logFilename
      = logDir+File.separator+"goldfishExtract_"+
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

  private String oracleDate (String sqlServerDate)
  {
    String result = "";
    if (!(sqlServerDate==null))
    {
      result =
        sqlServerDate.substring(0,4)+HYPHEN+
        monthName(sqlServerDate.substring(5,7))+HYPHEN+
        sqlServerDate.substring(8,10);
    }
    return result;
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

  // work out month number from text value
  private String monthName( String monthValue)
  {
    String result = "";
    if (monthValue.startsWith("01"))
      result = "Jan";
    else if (monthValue.startsWith("02"))
      result = "Feb";
    else if (monthValue.startsWith("03"))
      result = "Mar";
    else if (monthValue.startsWith("04"))
      result = "Apr";
    else if (monthValue.startsWith("05"))
      result = "May";
    else if (monthValue.startsWith("06"))
      result = "Jun";
    else if (monthValue.startsWith("07"))
      result = "Jul";
    else if (monthValue.startsWith("08"))
      result = "Aug";
    else if (monthValue.startsWith("09"))
      result = "Sep";
    else if (monthValue.startsWith("10"))
      result = "Oct";
    else if (monthValue.startsWith("11"))
      result = "Nov";
    else if (monthValue.startsWith("12"))
      result = "Dec";
    return result;
  }

  private void closeOracleConnection(Connection conn)
  {
    try
    {
      conn.close();
    }
    catch( Exception e)
    {
      if (!(e.toString().startsWith("java.lang.NullPointerException")))
      {
        message = "   Error closing connection to Oracle : " + e.toString();
        System.out.println(message);
        writeToLogFile(message);
      }
    }
  }

  private boolean openOracleConnection(int connNo)
  {
    boolean result = true;
    String url = "jdbc:oracle:thin:@"+oracleServer+":"+oraclePort+":"+oracleDb;
    try
    {
      Class.forName("oracle.jdbc.driver.OracleDriver");
      if (connNo==1)
        oracleConn = DriverManager.getConnection(url,oracleUser,oraclePassword);
      else if (connNo==2)
        oracleConn2 = DriverManager.getConnection(url,oracleUser,oraclePassword);
      else if (connNo==3)
        oracleConn3 = DriverManager.getConnection(url,oracleUser,oraclePassword);
      else
      {
        message = "   Invalid connection number "+connNo+" in openOracleConnection";
        System.out.println(message);
        writeToLogFile(message);
        result = false;
      }
    }
    catch( Exception e)
    {
      message = "   Error opening connection to Oracle : " + e.toString();
      System.out.println(message);
      writeToLogFile(message);
      result = false;
    }
    return result;
  }

  // returns current date/time in dd/mm/yyyy hh:mi:ss format
  private String currentDT()
  {
    String DateTime = new java.util.Date().toString();
    String reformatDT =
      DateTime.substring(8,10)+"/"+
      decodeMonth(DateTime.substring(4,7))+"/"+
      DateTime.substring(24,28)+" "
      +DateTime.substring(11,13)+":"+
      DateTime.substring(14,16)+":"+
      DateTime.substring(17,19);
    return reformatDT;
  }

  // returns current date/time in dd/mm/yyyy hh:mi:ss.999 format
  private String timestamp()
  {
    String DateTime = new java.util.Date().toString();
    long justTime = new java.util.Date().getTime();
    String mSecs = Long.toString(justTime);
    String reformatDT =
      decodeMonth(DateTime.substring(4,7))+"/"+
      DateTime.substring(8,10)+"/"+
      DateTime.substring(24,28)+" "
      +DateTime.substring(11,13)+":"+
      DateTime.substring(14,16)+":"+
      DateTime.substring(17,19)+"."+
      mSecs.substring(mSecs.length()-3,mSecs.length());
    return reformatDT;
  }

}



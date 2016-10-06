package batch;

import java.io.*;
import java.sql.*;

public class wlrUpdate {

  static Connection conn = null;
  static Statement st = null;
  static ResultSet rs = null;

  wlrUpdate(){}

  public static void main(String[] args)
  {

    // get property values
    String dbServer = "";
    String dbName = "";
    String username = "";
    String password = "";
    String URL = "";
    String mode = "";
    String dropDir = "";
    String logDir = "";
    String procDir = "";
    String message = "";
    String vrate = "";
    double vatrate = 0.00;

    System.out.print("Started WLR Update process");
    try {

      FileReader properties = new FileReader("GCB.properties");
      BufferedReader buffer = new BufferedReader(properties);

      boolean eofproperties = false;
      String propline = buffer.readLine();
      String propname = propline.substring(0,8);
      int lineLength = propline.length();
      String propval = propline.substring(9,lineLength).trim();

      while (!eofproperties) {

        if (propname.equals("dbserver"))
          dbServer = propval;

        if (propname.equals("database"))
          dbName = propval;

        if (propname.equals("username"))
          username = propval;

        if (propname.equals("password"))
          password = propval;

        if (propname.equals("wmode   "))
          mode = propval;

        if (propname.equals("wdrop   "))
          dropDir = propval;

        if (propname.equals("wlog    "))
          logDir = propval;

        if (propname.equals("wproc   "))
          procDir = propval;

        if (propname.equals("vatrate "))
        {
          vrate = propval;
          vatrate = Double.parseDouble(vrate);
        }

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
        System.out.println("Error accessing properties file --- " + e.toString());
    }
    System.out.print(" in "+mode+" mode\n");

    // open DB connection
    String url = "jdbc:AvenirDriver://"+dbServer+":1433/"+dbName;
    try
    {
      Class.forName("net.avenir.jdbcdriver7.Driver");
      conn = DriverManager.getConnection(url,username,password);
    }
    catch( Exception e)
    {
      System.out.println("Error opening database connection-- " + e.toString());
    }

    // open log file
    BufferedWriter logFile = openLogFile(logDir);
    String startDateTime = new java.util.Date().toString();
    String reformatSDT =
      startDateTime.substring(24,28)+"/"+decodeMonth(startDateTime.substring(4,7))+"/"+
      startDateTime.substring(8,10)+" "+startDateTime.substring(11,13)+":"+
      startDateTime.substring(14,16)+":"+startDateTime.substring(17,19);
    writeToLogFile(logFile,"WLR Update started at "+reformatSDT);

    // process drop files
    File dropFolder = new File(dropDir);
    if (dropFolder.exists())
    {
      String[] dropFiles = dropFolder.list();
      if (dropFiles.length>0)
      {
        // process each drop file
        for( int k=0; k < dropFiles.length; k++)
        {
          // process update file
          String updateFilename = dropDir+File.separator+dropFiles[k];
          File dropFile = new File(updateFilename);
          message = "   Processing file "+updateFilename;
          System.out.println(message);
          writeToLogFile(logFile,message);

          // process drop file
          BufferedReader dropReader = openDropFile(dropFile);
          String line = readDropFile(dropReader);
          String invoiceNo = "";
          String accountNo ="", invoiceTotal ="", custId = "", billProductId = "", billPeriodRef = "";
          double invoiceTotalValue = 0.00, checkTotalValue = 0.00;
          String lineColumns[] = decodeLine(line);
          String billingNo = "", quantity = "", desc = "", chargeAmount = "";
          String chargeStartDate = "", chargeEndDate = "";
          boolean dataOK = true;
          int lineCount = 1;
          boolean eof = false;
          while (!eof)
          {
            if (line==null)
            {
              eof=true;
              if (lineCount==1)
              {
                message = "      drop file "+dropFile.getName()+" is empty!";
                System.out.println(message);
                writeToLogFile(logFile,message);
              }
            }
            else
            {
              lineColumns = decodeLine(line);
              if (lineCount==1)
              {
                // 1st line holding invoice details
                if ((!lineColumns[0].toLowerCase().startsWith("inv")))
                {
                  dataOK=false;
                  message="      missing invoice header line";
                  System.out.println(message);
                  writeToLogFile(logFile,message);
                }
                else
                {
                  // get invoice number from invoice header line
                  invoiceNo = lineColumns[0].trim().substring(lineColumns[0].length()-8,lineColumns[0].length()).trim();
                  if (invoiceNo.length()!=8)
                  {
                    dataOK=false;
                    message="      unable to determine invoice number";
                    System.out.println(message);
                    writeToLogFile(logFile,message);
                  }
                  else
                  {
                    // get source invoice details
                    String[] siDetails = new String[5];
                    siDetails = getInvoiceDetails(invoiceNo);
                    accountNo = siDetails[0].substring(0,8);
                    invoiceTotal = siDetails[1].trim();
                    invoiceTotalValue = Double.parseDouble(invoiceTotal);
                    custId = siDetails[2].trim();
                    billProductId = siDetails[3].trim();
                    billPeriodRef = siDetails[4].trim();
                    if (accountNo.equals(""))
                    {
                      dataOK=false;
                      message="      unable to get source invoice details from DB";
                      System.out.println(message);
                      writeToLogFile(logFile,message);
                    }
                    else
                    {
                      // truncate work table if scan mode is update
                      if (mode.startsWith("update"))
                      {
                        if (!truncateWorkTable())
                        {
                          dataOK=false;
                          message="      unable to truncate work table";
                          System.out.println(message);
                          writeToLogFile(logFile,message);
                        }
                      }
                    }
                  }
                }
              }
              else if (lineCount==2)
              {
                // 2nd line holding column names - ignore!
              }
              else
              {
                // subsequent lines
                String suppliedAccountNo = lineColumns[0].trim();
                if (suppliedAccountNo.length()==7)
                  suppliedAccountNo = "0"+suppliedAccountNo;
                  billingNo = lineColumns[1];
                  quantity = lineColumns[7];
                  desc = lineColumns[8];
                  chargeAmount = lineColumns[11];
                  double chargeAmountValue = Double.parseDouble(chargeAmount);
                  checkTotalValue = checkTotalValue + chargeAmountValue;
                  chargeStartDate = lineColumns[9];
                  chargeEndDate = lineColumns[10];
                  // check that supplied account number matches
                  // account number from source invoice
                  if (!suppliedAccountNo.startsWith(accountNo))
                  {
                    dataOK=false;
                    message="      mismatched account number of "+suppliedAccountNo+" on line "+lineCount;
                    System.out.println(message);
                    writeToLogFile(logFile,message);
                  }
                  // if mode is update add charge details to the work table
                  if (mode.startsWith("update"))
                  {
                    if (!insertWorkCharge
                          (custId,billProductId,billPeriodRef,invoiceNo,accountNo,
                           chargeStartDate,chargeEndDate,billingNo,desc,quantity,
                           chargeAmountValue,vatrate))
                    {
                      dataOK=false;
                      message="      error writing to work table";
                      System.out.println(message);
                      writeToLogFile(logFile,message);
                    }
                  }
              }
              line = readDropFile(dropReader);
              lineCount++;
            }
          }
          double checkDiff = invoiceTotalValue - checkTotalValue;
          if ((checkDiff>=0.01)||(checkDiff<=-0.01))
          {
            message =
              "   source invoice rental total of "+invoiceTotalValue+
              "does not match check rental total of "+checkTotalValue;
            System.out.println(message);
            writeToLogFile(logFile,message);

          }
          closeDropFile(dropReader);

          if (mode.startsWith("update"))
          {
            if (dataOK)
            {
              if (updateWLR(invoiceNo))
              {
                message="      WLR update has been successfully performed";
                System.out.println(message);
                writeToLogFile(logFile,message);
              }
              else
              {
                message="      WLR update has failed";
                System.out.println(message);
                writeToLogFile(logFile,message);
              }
            }
            else
            {
              message="      cannot complete update due to errors in the WLR update file";
              System.out.println(message);
              writeToLogFile(logFile,message);
            }
          }

          // move processed update file to processed directory
          String newFilename = procDir+File.separator+dropFiles[k];
          File newFile = new File(newFilename);
          if (!dropFile.renameTo(newFile))
            {
              message = "   Failed to move file "+updateFilename+" to "+newFilename;
              System.out.println(message);
              writeToLogFile(logFile,message);
            }
        }
      }
      else
      {
        message = "   No files to process in "+dropDir;
        System.out.println(message);
        writeToLogFile(logFile,message);
      }
    }
    else
    {
      message = "Drop directory "+dropDir+" not found";
      System.out.println(message);
      writeToLogFile(logFile,message);
    }

    // close DB connection
    try
    {
      conn.close();
    }
    catch( Exception e)
    {
      System.out.println("Error closing database connection-- " + e.toString());
    }

    // Close log file
    String endDateTime = new java.util.Date().toString();
    String reformatEDT =
      endDateTime.substring(24,28)+"/"+decodeMonth(endDateTime.substring(4,7))+"/"+
      endDateTime.substring(8,10)+" "+endDateTime.substring(11,13)+":"+
      endDateTime.substring(14,16)+":"+endDateTime.substring(17,19);
    writeToLogFile(logFile,"WLR Update finished at "+reformatEDT);
    closeLogFile(logFile);

    System.out.println("Finished WLR Update process");
  }

  static BufferedWriter openLogFile( String logDir )
  {
  // Open log file
    BufferedWriter result = null;
    String logDate = new java.util.Date().toString();
    String logFilename
      = logDir+File.separator+"WLR_Update_"+
        logDate.substring(24,28)+decodeMonth(logDate.substring(4,7))+
        logDate.substring(8,10)+"_"+logDate.substring(11,13)+
        logDate.substring(14,16)+logDate.substring(17,19)+
        "_log.txt";
    try
    {
      FileWriter logFile = new FileWriter(logFilename);
      BufferedWriter logBuffer = new BufferedWriter(logFile);
      result = logBuffer;
    }
    catch (IOException e)
    {
      System.out.println("   Error opening log file --- " + e.toString());
    }
    return result;
  }

  static void writeToLogFile( BufferedWriter logFile, String line )
  {
    try
    {
      logFile.write(line+"\r\n");
    }
    catch (IOException e)
    {
      System.out.println("   Error writing to log file --- " + e.toString());
    }
  }

  static String readDropFile( BufferedReader dropFileReader )
  {
    String result ="";
    try
    {
      result = dropFileReader.readLine();
    }
    catch (IOException e)
    {
      System.out.println("   Error reading drop file --- " + e.toString());
    }
    return result;
  }

  static void closeLogFile( BufferedWriter logFile )
  {
    try
    {
      logFile.close();
    }
    catch (IOException e)
    {
      System.out.println("   Error closing log file --- " + e.toString());
    }
  }

  static void closeDropFile( BufferedReader dropFileReader )
  {
    try
    {
      dropFileReader.close();
    }
    catch (IOException e)
    {
      System.out.println("   Error closing drop file --- " + e.toString());
    }
  }

  static BufferedReader openDropFile( File dropFile )
  {
  // Open drop file
    BufferedReader result = null;
    try
    {
      BufferedReader dropBuffer = new BufferedReader(new FileReader(dropFile));
      result = dropBuffer;
    }
    catch (IOException e)
    {
      System.out.println("   Error opening drop file --- " + e.toString());
    }
    return result;
  }

  static String[] decodeLine( String tabLine )
  {
    String[] result = new String[50];
    String work = "", testChar = "";
    int columnNo = 0;
    for( int i = 0; i < tabLine.length(); i++)
    {
      testChar = tabLine.substring(i,i+1);
      // columns are delimted by tabs
      if (testChar.startsWith("\t"))
      {
        result[columnNo] = work;
        work = "";
        columnNo++;
      }
      else
      {
        work = work + testChar;
      }
    }
    result[columnNo] = work;
    return result;
  }

  static String decodeMonth( String month)
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

  static boolean truncateWorkTable ()
  {
    boolean result = false;
    String SQL = "TRUNCATE TABLE Source_Recurring_Charge_Work";
    try
    {
      st = conn.createStatement();
      if (st.execute(SQL))
        result = true;
      else
        result = true;
    }
    catch( Exception e)
    {
      System.out.println(
        "Error truncating table Source_Recurring_Work "+" : "+e.toString());
    }
    return result;
  }

  static boolean insertWorkCharge
    (String custId, String productId, String periodRef,
     String sourceInvoiceNo, String sourceAccountNo,
     String startDate, String endDate,
     String circuitRef, String desc, String quantity,
     double chargeAmount,
     double vrate )
  {
    boolean result = false;
    double vat = chargeAmount * vrate;
    double total = chargeAmount + vat;
    String SQL =
      "INSERT INTO Source_Recurring_Charge_Work VALUES("+
      custId+",'"+productId+"','"+periodRef+"','"+
      sourceInvoiceNo+"','"+sourceAccountNo+"','VREN',"+
      "CONVERT(DATETIME,'"+startDate+"',103),"+
      "CONVERT(DATETIME,'"+endDate+"',103),'"+
      circuitRef+"','"+desc+"',"+quantity+","+
      chargeAmount+","+vat+","+total+")";
    //System.out.println(SQL);
    try
    {
      st = conn.createStatement();
      if (st.execute(SQL))
        result = true;
      else
        result = true;
    }
    catch( Exception e)
    {
      System.out.println(
        "Error truncating inserting into table Source_Recurring_Work : "+
        SQL+" : "+e.toString());
    }
    return result;
  }

  static boolean updateWLR ( String sourceInvoiceNo)
  {
    boolean result = false;
    String SQL = "exec wlr_update '"+sourceInvoiceNo+"' ";
    try
    {
      st = conn.createStatement();
      if (st.execute(SQL))
        result = true;
      else
        result = true;
    }
    catch( Exception e)
    { System.out.println(
        "Error running procedure wlr_update for "+sourceInvoiceNo+" : "+
        SQL+" : "+e.toString());
    }
    return result;
  }

  static String[] getInvoiceDetails( String sourceInvoiceNo )
  {
    // returns source account number and rental total for source invoice
    String results[] = new String[5];
    String accountNo = "", rentalTotal = "", custId = "", productId = "", billPeriodRef = "";
    String SQL =
      "SELECT Source_Account_No, CONVERT(varchar(20),Rental_Charges), "+
      "CONVERT(varchar(20),Conglom_Cust_Id), Billed_Product_Id, Bill_Period_Ref "+
      "FROM Source_Invoice "+
      "WHERE Source_Invoice_no = '"+sourceInvoiceNo+"' ";
    //System.out.println(SQL);
    try
    {
      st = conn.createStatement();
      rs = st.executeQuery(SQL);
      if (rs.next())
        accountNo = rs.getString(1);
        rentalTotal = rs.getString(2);
        custId = rs.getString(3);
        productId = rs.getString(4);
        billPeriodRef = rs.getString(5);
    }
    catch( Exception e)
    {
      System.out.println(
        "Error getting source invoice details for source invoice "+
        sourceInvoiceNo+" : "+e.toString());
    }
    results[0] = accountNo;
    results[1] = rentalTotal;
    results[2] = custId;
    results[3] = productId;
    results[4] = billPeriodRef;
    return results;
  }

}


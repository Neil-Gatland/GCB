package batch;

import java.io.*;
import java.sql.*;

/**
 * Title:        LoadTalismanBIF
 * Description:  Load Talisman BIF File data into Conglomerate source tables
 * Copyright:    Copyright (c) 22 May 2006
 * Company:      Danet Ltd
 * @author       Neil Gatland
 * @version      1.0
 */

public class LoadTalismanBIF {

  LoadTalismanBIF(){}

  public static void main(String[] args) {

    String dbServer = "";
    String dbName = "";
    String username = "";
    String password = "";
    String URL = "";
    String BIFFileName ="";
    String SQL = "";
    String vrate = "";
    double vatrate = 0.00;

    Connection conn = null;
    Statement st = null;
    ResultSet rs = null;

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

    } catch (IOException e) {
        System.out.println("Error accessing properties file --- " + e.toString());
    }

    String url = "jdbc:AvenirDriver://"+dbServer+":1433/"+dbName;

    try {
      Class.forName("net.avenir.jdbcdriver7.Driver");
      conn = DriverManager.getConnection(url,username,password);

    } catch( Exception e) {
        System.out.println("Error on database connection-- " + e.toString());
    }

    if (args.length==0)
      System.out.println("No BIF file name supplied");
    else {
      BIFFileName = args[0];
      System.out.println("Processing BIF file " + BIFFileName);

      try {

        FileReader file = new FileReader(BIFFileName);
        BufferedReader buff = new BufferedReader(file);

        boolean eof = false;
        boolean TrailerReached = false;
        boolean EndOfInvoice = true;
        boolean ProcessBlock = false;

        String line = buff.readLine();
        String RecordType = "";

        String Account_Number = "";
        String Invoice_Number = "";
        String Source_Invoice_No = "";
        String Invoice_Start_Date = "";
        String Invoice_End_Date = "";
        String Invoice_Date = "";
        String Billing_Ref_Prefix = "";
        String Bill_Period_Ref = "";
        String Internal_Reference = "";
        String RC_Description = "";
        String RC_Start_Date = "";
        String RC_End_Date = "";
        String VAT_Rate_Applicable = "";
        String Unit_Price = "";
        String Number_Of_Units = "";
        String Total_Recurring_Charge = "";
        String Usage_Description = "";
        String Usage_Start_Date = "";
        String Usage_End_Date = "";
        String Total_Usage_Charge = "";
        String NRC_Description = "";
        String NRC_Start_Date = "";
        String Total_NRC_Charge = "";
        String YearValue = "";
        String MonthValue = "";
        String InvoiceSuffix = "";

        int Conglom_Cust_Id = 0;
        int Year = 0;
        int Month = 0;

        double WorkAmount = 0.00;
        double Invoice_Net_Amount = 0.00;
        double Invoice_VAT_Amount = 0.00;
        double Invoice_Total_Amount = 0.00;
        double One_Off_Charges_Total = 0.00;
        double Recurring_Charges_Total = 0.00;
        double Usage_Charges_Total = 0.00;

        // Check that the file is not empty and starts with an
        // AAAA file header record
        if (line == null)
          System.out.println("-- File is empty ");
        else {
          RecordType = line.substring(0,4);
          if (!(RecordType.equals("AAAA"))) {
            System.out.println("BIF file is missing it's header record");
            eof = true;
          }
        }

        while (!eof) {
          line = buff.readLine();

          // if eof check that final record on BIF is ZZZZ trailer record
          if (line == null) {
            eof = true;
            if (!(RecordType.equals("ZZZZ")))
              System.out.println("BIF file is missing it's final trailer record");
          }

          else {
            RecordType = line.substring(0,4);

            // Ensure that all invoices block start with
            // a 1000 invoice block header record
            if ( (EndOfInvoice) &&
                 (!(RecordType.equals("1000"))) &&
                    (!(RecordType.equals("ZZZZ"))) ) {
              System.out.println("Expected invoice header missing");
              eof = true;
            }

            // Check for end of invoice block
            if (RecordType.equals("9999")) {
              EndOfInvoice = true;
              if (ProcessBlock) {
                // Write out source invoice record
                SQL = "INSERT INTO Source_Invoice " +
                      "(Conglom_Cust_Id,Billed_Product_Id,Bill_Period_Ref," +
                      "Source_Invoice_No,Source_Account_No,"+
                      "Billed_Date,Bill_Period_Start_Date,Bill_Period_End_Date,"+
                      "Conglom_Invoice_Ref,Status,Invoice_Net_Amount,"+
                      "Invoice_VAT_Amount,Invoice_Total_Amount,"+
                      "Acct_Bal_Net_Amount,Acct_Bal_VAT_Amount,"+
                      "Acct_Bal_Amount,Load_Method_Code,"+
                      "One_Off_Charges,Recurring_Charges,Usage_Charges,"+
                      "Misc_Charges,Adjustment_Total,Source_Disc_Total,"+
                      "Conglom_Disc_Total,Install_Charges,Rental_Charges,"+
                      "Callink_Charges,Call_Charges,VPN_Charges,Authcode_Charges,"+
                      "Easy_Usage_Charges,Easy_Qrtly_Charges,Sundry_Charges,"+
                      "Special_Charges,Credits_Total,Debit_Adjustmt_Total,"+
                      "Last_Update_Date,Last_Update_Id,Arbor_Talisman_Ind) "+
                      "VALUES("+
                      Conglom_Cust_Id+",'PCBL','"+Bill_Period_Ref+"'," +
                      "'"+Source_Invoice_No+"','"+Account_Number+"',"+
                      "getdate(),'"+SQLDate(Invoice_Start_Date)+"',"+
                      "'"+SQLDate(Invoice_End_Date)+"','"+Billing_Ref_Prefix+
                      "/"+InvoiceSuffix+"','OPEN',"+
                      Invoice_Net_Amount+","+Invoice_VAT_Amount+","+
                      Invoice_Total_Amount+","+Invoice_Net_Amount+","+
                      Invoice_VAT_Amount+","+Invoice_Total_Amount+",'T',"+
                      One_Off_Charges_Total+","+Recurring_Charges_Total+","+
                      Usage_Charges_Total+",0,0,0,0,"+One_Off_Charges_Total+","+
                      Recurring_Charges_Total+",0,0,0,0,0,0,0,0,0,0,"+
                      "getdate(),'Arbor Talisman Extract','Y')";
                try {
                  st = conn.createStatement();
                  st.execute(SQL);
                } catch( Exception e) {
                    System.out.println(
                      "Error inserting Source_Invoice -- " +
                      e.toString());
                }

              System.out.println(
                "Completed processing of invoice "+Invoice_Number);

            }
            else
              EndOfInvoice = false;
            }

            // Check for ZZZZ trailer record not at the end of the BIF file
            if (TrailerReached) {
              System.out.println("BIF file trailer record is not at the end");
              eof = true;
            }

            // Check for a AAAA file header record not at the start of the BIF
            if (RecordType.equals("AAAA")) {
              System.out.println("BIF file has more than one header");
              eof = true;
            }

           // Check for ZZZZ trailer record
            if (RecordType.equals("ZZZZ"))
              TrailerReached = true;

            // Process an Invoice Info 1000 record
            if  (RecordType.equals("1000")) {

              // Store required data
              //Account_Number = line.substring(647,694).trim();
              Account_Number = line.substring(827,874).trim();
              //Invoice_Number = line.substring(695,719).trim();
              Invoice_Number = line.substring(971,995).trim();
              //System.out.println("Account_Number = "+Account_Number);
              //System.out.println("Invoice_Number = "+Invoice_Number);
              Source_Invoice_No = SourceInvoiceNo(Invoice_Number);
              //Invoice_Start_Date = line.substring(734,742);
              //Invoice_End_Date = line.substring(742,750);
              //Invoice_Date = line.substring(750,758);
              Invoice_Start_Date = line.substring(1065,1073);
              Invoice_End_Date = line.substring(1073,1081);
              Invoice_Date = line.substring(1081,1089);
              MonthValue = Invoice_Start_Date.substring(2,4);
              Month = Integer.parseInt(MonthValue);
              YearValue = Invoice_Start_Date.substring(6,8);
              Year = Integer.parseInt(YearValue);
              if ( Invoice_Start_Date.substring(2,4).startsWith("12") )
              {
                Year = Year + 1;
                Month = 1;
              }
              else
              {
                Month = Month + 1;
              }
              if (Year>9)
              {
                YearValue = Integer.toString(Year);
              }
              else
              {
                YearValue = "0" + Integer.toString(Year);
              }
              if (Month>9)
              {
                MonthValue = Integer.toString(Month);
              }
              else
              {
                MonthValue = "0" + Integer.toString(Month);
              }
              Bill_Period_Ref = YearValue+MonthValue;
              InvoiceSuffix=MonthValue+YearValue;

              // See if account number is on the Arbor Account Mapping table
              SQL = "SELECT c.Conglom_Cust_Id, c.Billing_Ref_Prefix " +
                    "FROM Conglom_Customer c, Arbor_Account_Mapping a " +
                    "WHERE a.Arbor_Account_No = '"+Account_Number+"' " +
                    "AND a.Conglom_Cust_Id = c.Conglom_Cust_Id";
              try {

                st = conn.createStatement();
                rs = st.executeQuery(SQL);

                try {
                  rs.next();
                  Conglom_Cust_Id = rs.getInt("Conglom_Cust_Id");
                  Billing_Ref_Prefix = rs.getString("Billing_Ref_Prefix").trim();
                  ProcessBlock = true;
                  System.out.println("Loading data for invoice "+
                    Invoice_Number+
                     " (account "+
                     Account_Number+
                     ")");
                  SQL =
                    "EXEC Clear_Arbor_Source_Data "+
                    Conglom_Cust_Id+",'"+Bill_Period_Ref+"'";
                  try {
                    st.execute(SQL);
                  } catch (Exception e) {
                      System.out.println(
                        "Error clearing Arbor source data -- "+
                        e.toString());
                      ProcessBlock=false;
                  }
                } catch( Exception e) {
                    ProcessBlock = false;
                    System.out.println("Ignoring invoice "+
                      Invoice_Number+
                      " account "+
                      Account_Number+
                      " is not mapped");
                }

              } catch( Exception e) {
                  System.out.println("Error mapping Arbor Account -- " + e.toString());
              }

              // Reset invoice totals and end of invoice boolean
              Invoice_Net_Amount = 0.00;
              Invoice_VAT_Amount = 0.00;
              Invoice_Total_Amount = 0.00;
              One_Off_Charges_Total = 0.00;
              Recurring_Charges_Total = 0.00;
              Usage_Charges_Total = 0.00;
              EndOfInvoice = false;

            }

            // Only process other record types if account is held
            // on the Arbor Account Mapping table
            if ( ProcessBlock ) {

              // Process a Site Prod Identifier 3110 record
              if  (RecordType.equals("3110")) {

                // Store internal reference (includes surrounding brackets)
                //Internal_Reference = line.substring(145).trim();
                //Internal_Reference = line.substring(242,253).trim();
                Internal_Reference = line.substring(234,253).trim();
                SQL = "SELECT Circuit_Reference "+
                      "FROM   Arbor_IR_Mapping "+
                      "WHERE  Internal_Reference = '"+
                      Internal_Reference+"'";
                try
                {
                  st = conn.createStatement();
                  rs = st.executeQuery(SQL);
                  try
                  {
                    rs.next();
                    Internal_Reference = rs.getString("Circuit_Reference");
                  }
                  catch( Exception e)
                  {
                    Internal_Reference = "NO MAPPING RECORD";
                  }
                }
                catch( Exception e)
                {
                    System.out.println(
                      "Error reading Arbor_IR_Mapping -- " + SQL + " : "+
                      e.toString());
                }

              }

              // Process RC and RC Credit records
              if  ((RecordType.equals("3210"))||(RecordType.equals("3231")))
              {

                // Store required data
                if (RecordType.equals("3231"))
                {
                  RC_Description = line.substring(14,94).trim();
                  RC_Start_Date = SQLDate(line.substring(254,262));
                  RC_End_Date = SQLDate(line.substring(254,262));
                  VAT_Rate_Applicable = line.substring(262,268).trim();
                  Unit_Price = line.substring(268,286).trim();
                  Number_Of_Units = "1";
                  Total_Recurring_Charge = line.substring(268,286).trim();
                }
                else
                {
                  RC_Description = line.substring(6,86).trim();
                  RC_Start_Date = SQLDate(line.substring(86,94));
                  RC_End_Date = SQLDate(line.substring(94,102));
                  VAT_Rate_Applicable = line.substring(102,108).trim();
                  Unit_Price = line.substring(108,126).trim();
                  Number_Of_Units = line.substring(126,132).trim();
                  Total_Recurring_Charge = line.substring(132,151).trim();
                }

                // Increment invoice totals
                WorkAmount = Double.parseDouble(Total_Recurring_Charge);
                Invoice_Net_Amount = Invoice_Net_Amount + WorkAmount;
                Invoice_VAT_Amount = Invoice_VAT_Amount + (WorkAmount * vatrate);
                Invoice_Total_Amount = Invoice_Total_Amount + (WorkAmount * (1 + vatrate));
                Recurring_Charges_Total = Recurring_Charges_Total + WorkAmount;

                // Create a Source_Recurring_Charge_Record
                SQL = "INSERT INTO Source_Recurring_Charge " +
                      "(Conglom_Cust_Id,Billed_Product_Id,Bill_Period_Ref," +
                      "Source_Invoice_No,Source_Account_No,Charge_Type_Code,"+
                      "Period_Start_Date,Period_End_Date,Circuit_Description,"+
                      "Quantity,Net_Amount,VAT_Amount,Total_Amount,"+
                      "Last_Update_Date,Last_Update_Id,Arbor_Talisman_Ind,"+
                      "Internal_Reference) VALUES("+
                      Conglom_Cust_Id+",'PCBL','"+Bill_Period_Ref+"'," +
                      "'"+Source_Invoice_No+"','"+Account_Number+"','PREN',"+
                      "'"+RC_Start_Date+"','"+RC_End_Date+
                      "','"+RC_Description+"',"+
                      ""+Number_Of_Units+","+WorkAmount+","+
                      (WorkAmount * vatrate)+","+(WorkAmount * (1+vatrate))+","+
                      "getdate(),'Arbor Talisman Extract','Y',"+
                      "REPLACE(REPLACE('"+Internal_Reference+"','(',''),')',''))";
                try {
                  st = conn.createStatement();
                  st.execute(SQL);
                } catch( Exception e) {
                    System.out.println(
                      "Error inserting Source_Recurring_Charge -- " +
                      e.toString());
                }

              }

              // Process a Usage and credit usage recors
              if  ((RecordType.equals("3320"))||(RecordType.equals("3361")))
              {

                if (RecordType.equals("3361"))
                {
                  Usage_Description = line.substring(14,94).trim();
                  Usage_Start_Date = SQLDate(line.substring(254,262));
                  Usage_End_Date = SQLDate(line.substring(254,262));
                  Total_Usage_Charge = line.substring(268,286).trim();

                }
                else
                {
                  Usage_Description = line.substring(12,92).trim();
                  Usage_Start_Date = SQLDate(line.substring(92,100));
                  Usage_End_Date = SQLDate(line.substring(100,108));
                  Total_Usage_Charge = line.substring(114,132).trim();
                }

                // Increment invoice totals
                WorkAmount = Double.parseDouble(Total_Usage_Charge);
                Invoice_Net_Amount = Invoice_Net_Amount + WorkAmount;
                Invoice_VAT_Amount = Invoice_VAT_Amount + (WorkAmount * vatrate);
                Invoice_Total_Amount = Invoice_Total_Amount + (WorkAmount * (1+vatrate));
                Usage_Charges_Total = Usage_Charges_Total + WorkAmount;

                // Create a Source_Usage_Other_Charge_Record
                SQL = "INSERT INTO Source_Usage_Other_Charge " +
                      "(Conglom_Cust_Id,Billed_Product_Id,Bill_Period_Ref," +
                      "Source_Invoice_No,Source_Account_No,Charge_Type_Code,"+
                      "Charge_Start_Date,Charge_End_Date,Charge_Description,"+
                      "Charge_Amount,Last_Update_Date,Last_Update_Id,"+
                      "Arbor_Talisman_Ind,Internal_Reference) VALUES("+
                      Conglom_Cust_Id+",'PCBL','"+Bill_Period_Ref+"'," +
                      "'"+Source_Invoice_No+"','"+Account_Number+"','PUSG',"+
                      "'"+Usage_Start_Date+"','"+Usage_End_Date+
                      "','"+Usage_Description+"',"+WorkAmount+","+
                      "getdate(),'Arbor Talisman Extract','Y',"+
                      "REPLACE(REPLACE('"+Internal_Reference+"','(',''),')',''))";
                try {
                  st = conn.createStatement();
                  st.execute(SQL);
                } catch( Exception e) {
                    System.out.println(
                      "Error inserting Source_Usage_Other_Charge -- " +
                      e.toString());
                }

              }

              // Process a NRC and Credit NRC
              if  ((RecordType.equals("3270"))||(RecordType.equals("3281"))) {

                // Store required data
                if (RecordType.equals("3361"))
                {
                  NRC_Description = line.substring(14,94).trim();
                  NRC_Start_Date = SQLDate(line.substring(254,262));
                  Number_Of_Units = "1";
                  Total_NRC_Charge = line.substring(268,286).trim();

                }
                else
                {
                  NRC_Description = line.substring(6,86).trim();
                  NRC_Start_Date = SQLDate(line.substring(86,94));
                  Number_Of_Units = line.substring(118,124);
                  Total_NRC_Charge = line.substring(124,142).trim();
                }

                // Increment invoice totals
                WorkAmount = Double.parseDouble(Total_NRC_Charge);
                Invoice_Net_Amount = Invoice_Net_Amount + WorkAmount;
                Invoice_VAT_Amount = Invoice_VAT_Amount + (WorkAmount * vatrate);
                Invoice_Total_Amount = Invoice_Total_Amount + (WorkAmount * (1+vatrate));
                One_Off_Charges_Total = One_Off_Charges_Total + WorkAmount;

                // Create a Source_One_Off_Charge_Record
                SQL = "INSERT INTO Source_One_Off_Charge " +
                      "(Conglom_Cust_Id,Billed_Product_Id,Bill_Period_Ref," +
                      "Source_Invoice_No,Source_Account_No,Charge_Type_Code,"+
                      "Charge_Date,Charge_Description,Charge_Quantity,"+
                      "Charge_Amount,Last_Update_Date,Last_Update_Id,"+
                      "Arbor_Talisman_Ind,Internal_Reference) VALUES("+
                      Conglom_Cust_Id+",'PCBL','"+Bill_Period_Ref+"'," +
                      "'"+Source_Invoice_No+"','"+Account_Number+"','PINS',"+
                      "'"+NRC_Start_Date+"','"+NRC_Description+"',"+
                      Number_Of_Units+","+WorkAmount+","+
                      "getdate(),'Arbor Talisman Extract','Y',"+
                      "REPLACE(REPLACE('"+Internal_Reference+"','(',''),')',''))";
                try {
                  st = conn.createStatement();
                  st.execute(SQL);
                } catch( Exception e) {
                    System.out.println(
                      "Error inserting Source_One_Off_Charge -- " +
                      e.toString());
                }

              }

            }

          }

        }

        // close BIF and database connection
        buff.close();
        conn = null;

      System.out.println("Completed processing of BIF");

      } catch (IOException e) {
          System.out.println("Error accessing BIF file --- " + e.toString());
      }

    }

  }

  static String SQLDate(String ArborDate) {

      String Day = ArborDate.substring(0,2);
      String Year = ArborDate.substring(4,8);
      int Month = Integer.parseInt(ArborDate.substring(2,4));
      String MonthName = "";
      switch (Month) {
        case 1:
          MonthName = "January";
          break;
        case 2:
          MonthName = "February";
          break;
        case 3:
          MonthName = "March";
          break;
        case 4:
          MonthName = "April";
          break;
        case 5:
          MonthName = "May";
          break;
        case 6:
          MonthName = "June";
          break;
        case 7:
          MonthName = "July";
          break;
        case 8:
          MonthName = "August";
          break;
        case 9:
          MonthName = "September";
          break;
        case 10:
          MonthName = "October";
          break;
        case 11:
          MonthName = "November";
          break;
        case 12:
          MonthName = "December";
          break;
        }
      return Day+" "+MonthName+" "+Year;
    }


  static String SourceInvoiceNo(String InvoiceNumber)
  {
    String fs = "/", zs = "", Reformatted = InvoiceNumber;
    if (Reformatted.startsWith("CN10/"))
      Reformatted =
        Reformatted.substring(5,Reformatted.length());
    if (Reformatted.startsWith("UK10/"))
      Reformatted =
        Reformatted.substring(5,Reformatted.length());
    Reformatted = Reformatted.substring(0,5)+Reformatted.substring(7,Reformatted.length());
    if (Reformatted.length()>10)
      Reformatted = Reformatted.substring(0,10);
    return Reformatted;
  }

}


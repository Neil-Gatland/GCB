package batch;

import java.io.*;
import java.sql.*;

public class sqlServerDB
{

  private Connection conn = null;
  private Statement st = null;
  private Statement st2 = null;
  private ResultSet rs = null;
  private String SQL;
  private String message;

  public sqlServerDB(Connection c)
  {
    conn = c;
  }

  // get result set of job queue entries
  public ResultSet getActiveJobs()
  {
    ResultSet rs = null;
    try
    {
      SQL =
        "SELECT cc.Conglom_Cust_Id, "+
        "cc.Conglom_Cust_Name, "+
        "bprc.Billed_Product_Id, "+
        "bprc.Bill_Period_Ref, "+
        "bprc.Extract_Type, "+
        "cc.VAT_Exempt_Ind, "+
        "bprc.Period "+
        "FROM Billing_Period_Run_Control bprc (nolock), Conglom_Customer cc (nolock) "+
        "WHERE cc.Conglom_Cust_Id = bprc.Conglom_Cust_Id "+
        "AND Active_Ind = 'Y'";
      st = conn.createStatement();
      rs = st.executeQuery(SQL);
    }
    catch(java.sql.SQLException ex)
    {
      System.out.println(SQL);
      message=ex.getMessage();
      System.out.println(message);
    }
    finally
    {
      return rs;
    }
  }

  // get vat rate as long (actually to 3 dec places multiplied by 1000
  public long getVatRate()
  {
    ResultSet rs = null;
    long result = 0;
    try
    {
      SQL =
        "SELECT Vat_Rate * 1000 "+
        "FROM Vat_Rate (nolock) "+
        "WHERE Effective_From <= GETDATE() "+
        "AND ( Effective_To IS NULL OR Effective_TO > GETDATE() )";
      st = conn.createStatement();
      rs = st.executeQuery(SQL);
      if (rs.next())
      {
        result = rs.getLong(1);
      }
      st.close();
    }
    catch(java.sql.SQLException ex)
    {
      System.out.println(SQL);
      message=ex.getMessage();
      System.out.println(message);
    }
    finally
    {
      st = null;
      rs = null;
      return result;
    }
  }

  // take long for 3 dec places and return long rounded to 2 dec places
  public long round3DecTo2(long dec3Value)
  {
    ResultSet rs = null;
    long result = 0;
    try
    {
      SQL = "SELECT ROUND(CONVERT( INTEGER, ROUND("+dec3Value+",2)),-1)/10";
      st = conn.createStatement();
      rs = st.executeQuery(SQL);
      if (rs.next())
      {
        result = rs.getLong(1);
      }
      st.close();
    }
    catch(java.sql.SQLException ex)
    {
      System.out.println(SQL);
      message=ex.getMessage();
      System.out.println(message);
    }
    finally
    {
      st = null;
      rs = null;
      return result;
    }
  }

  // get conglom discount
  public ResultSet getConglomDiscount(String conglomCustId, String billedProductId, String billPeriodRef, String sourceAccountNo)
  {
    ResultSet rs = null;
    try
    {
      SQL =
        "SELECT Discount_Item_Code1, Discount_Amount1 * 100 Discount_Amount1, Discountable_Value1 * 100 Discountable_Value1,  "+
        "Discount_Item_Code2, Discount_Amount2 * 100 Discount_Amount2, Discountable_Value2 * 100 Discountable_Value2,  "+
        "Discount_Item_Code3, Discount_Amount3 * 100 Discount_Amount3, Discountable_Value3 * 100 Discountable_Value3,  "+
        "Net_Amount * 100 Net_Amount, Vat_Amount * 100 Vat_Amount, Total_Amount * 100 Total_Amount "+
        "FROM Conglom_Discount (nolock) "+
        "WHERE Conglom_Cust_Id = "+conglomCustId+" "+
        "AND Billed_Product_Id = '"+billedProductId+"' "+
        "AND Bill_Period_Ref = '"+billPeriodRef+"' "+
        "AND Source_Account_No = '"+sourceAccountNo+"'";
      st = conn.createStatement();
      rs = st.executeQuery(SQL);
    }
    catch(java.sql.SQLException ex)
    {
      System.out.println(SQL);
      message=ex.getMessage();
      System.out.println(message);
    }
    finally
    {
      return rs;
    }
  }

  // get conglom adjustment
  public ResultSet getConglomAdjustment(String billedProductId, String sourceAccountNo, String creditNoteNumber)
  {
    ResultSet rs = null;
    try
    {
      SQL =
        "SELECT Net_Amount * 100 Net_Amount, Total_Amount * 100 Total_Amount "+
        "FROM Conglom_Adjustment (nolock) "+
        "WHERE Billed_Product_Id = '"+billedProductId+"' "+
        "AND Source_Account_No = '"+sourceAccountNo+"' "+
        "AND Docket_Number = '"+creditNoteNumber+"'";
      st = conn.createStatement();
      rs = st.executeQuery(SQL);
    }
    catch(java.sql.SQLException ex)
    {
      System.out.println(SQL);
      message=ex.getMessage();
      System.out.println(message);
    }
    finally
    {
      return rs;
    }
  }

  // get conglom discount
  public ResultSet getConglomDiscount(String billedProductId, String sourceAccountNo, String creditNoteNumber)
  {
    ResultSet rs = null;
    try
    {
      SQL =
        "SELECT Net_Amount * 100 Net_Amount, Total_Amount * 100 Total_Amount "+
        "FROM Conglom_Discount (nolock) "+
        "WHERE Billed_Product_Id = '"+billedProductId+"' "+
        "AND Source_Account_No = '"+sourceAccountNo+"' "+
        "AND Credit_Note_Number = '"+creditNoteNumber+"'";
      st = conn.createStatement();
      rs = st.executeQuery(SQL);
    }
    catch(java.sql.SQLException ex)
    {
      System.out.println(SQL);
      message=ex.getMessage();
      System.out.println(message);
    }
    finally
    {
      return rs;
    }
  }

  // get source invoice number for source account no in current period
  public ResultSet getLaSourceInvoice(String conglomCustId, String billedProductId, String billPeriodRef, String sourceAccountNo)
  {
    ResultSet rs = null;
    try
    {
      SQL =
        "SELECT Acct_Bal_Net_Amount, Acct_Bal_Vat_Amount, Acct_Bal_Amount, Conglom_Discount_Total "+
        "FROM Source_Invoice (nolock) "+
        "WHERE Conglom_Cust_Id = "+conglomCustId+" "+
        "AND Billed_Product_Id = '"+billedProductId+"' "+
        "AND Bill_Period_Ref = '"+billPeriodRef+"' "+
        "AND Source_Account_No = '"+sourceAccountNo+"'";
      st = conn.createStatement();
      rs = st.executeQuery(SQL);
    }
    catch(java.sql.SQLException ex)
    {
      System.out.println(SQL);
      message=ex.getMessage();
      System.out.println(message);
    }
    finally
    {
      return rs;
    }
  }

  // get source invoice number for source account no in current period
  public String getSourceInvoiceNo(String conglomCustId, String billedProductId, String billPeriodRef, String sourceAccountNo)
  {
    String sourceInvoiceNo = "";
    ResultSet rs = null;
    try
    {
      SQL =
        "SELECT Source_Invoice_No "+
        "FROM Source_Invoice (nolock) "+
        "WHERE Conglom_Cust_Id = "+conglomCustId+" "+
        "AND Billed_Product_Id = '"+billedProductId+"' "+
        "AND Bill_Period_Ref = '"+billPeriodRef+"' "+
        "AND Source_Account_No = '"+sourceAccountNo+"'";
      st = conn.createStatement();
      rs = st.executeQuery(SQL);
      if (rs.next())
        sourceInvoiceNo = rs.getString(1);
      st.close();
    }
    catch(java.sql.SQLException ex)
    {
      System.out.println(SQL);
      message=ex.getMessage();
      System.out.println(message);
    }
    finally
    {
      rs=null;
      return sourceInvoiceNo;
    }
  }

  // get source invoice number for source account no in current period
  public boolean updateLaSourceInvoice
    (String conglomCustId, String billedProductId, String billPeriodRef, String sourceAccountNo,
     long acctBalNetAmount, long acctBalVatAmount, long acctBalAmount, long conglomDiscTotal)
  {
    boolean result = false;
    try
    {
      SQL =
        "UPDATE Source_Invoice "+
        "SET Acct_Bal_Net_Amount = "+makeLongDecimal(acctBalNetAmount)+", "+
        "Acct_Bal_Vat_Amount = "+makeLongDecimal(acctBalVatAmount)+", "+
        "Acct_Bal_Amount = "+makeLongDecimal(acctBalAmount)+", "+
        "Conglom_Disc_Total = "+makeLongDecimal(conglomDiscTotal)+", "+
        "WHERE Conglom_Cust_Id = "+conglomCustId+" "+
        "AND Billed_Product_Id = '"+billedProductId+"' "+
        "AND Bill_Period_Ref = '"+billPeriodRef+"' "+
        "AND Source_Account_No = '"+sourceAccountNo+"'";
      st = conn.createStatement();
      st.executeQuery(SQL);
      st.close();
      result = true;
    }
    catch(java.sql.SQLException ex)
    {
      System.out.println(SQL);
      message=ex.getMessage();
      System.out.println(message);
    }
    finally
    {
      return result;
    }
  }

  // get source invoice number for source account no in current period
  public boolean updateSourceInvoiceForPCBCredit
    (String conglomCustId, String billedProductId, String billPeriodRef, String sourceAccountNo, String sourceInvoiceNo,
     long creditNet, long creditVat, long creditGross)
  {
    boolean result = false;
    try
    {
      SQL =
        "UPDATE Source_Invoice "+
        "SET Acct_Bal_Net_Amount = Acct_Bal_Net_Amount + "+makeLongDecimal(creditNet)+", "+
        "Acct_Bal_Vat_Amount = Acct_Bal_Vat_Amount + "+makeLongDecimal(creditVat)+", "+
        "Acct_Bal_Amount = Acct_Bal_Amount +"+makeLongDecimal(creditGross)+", "+
        "Adjustment_Total = Adjustment_Total"+makeLongDecimal(creditNet)+", "+
        "Credits_Total = Credits_Total"+makeLongDecimal(creditNet)+" "+
        "WHERE Conglom_Cust_Id = "+conglomCustId+" "+
        "AND Billed_Product_Id = '"+billedProductId+"' "+
        "AND Bill_Period_Ref = '"+billPeriodRef+"' "+
        "AND Source_Account_No = '"+sourceAccountNo+"' "+
        "AND Source_Invoice_No = '"+sourceInvoiceNo+"'";
      st = conn.createStatement();
      st.executeQuery(SQL);
      st.close();
      result = true;
    }
    catch(java.sql.SQLException ex)
    {
      System.out.println(SQL);
      message=ex.getMessage();
      System.out.println(message);
    }
    finally
    {
      return result;
    }
  }

  // get result set of view discounts
  public ResultSet getViewDiscounts(String conglomCustId, String billedProductId, String mthlyBillStartDate)
  {
    ResultSet rs = null;
    try
    {
      SQL =
        "SELECT Discount_Item_Code, "+
        "Whole_Value_Percent * 100 Whole_Value_Percent, "+
        "Discount_Rate * 100 Discount_Rate, "+
        "ISNULL( Lead_Account_Id, 'NULL') Lead_Account_Id, "+
        "Lead_Acct_Check_Digit, "+
        "Accts_Excluded_Ind, "+
        "Discount_Type_Decs, "+
        "Short_Description, "+
        "Seq_No "+
        "FROM View_Discounts (nolock) "+
        "WHERE Conglom_Cust_Id = '"+conglomCustId+"' "+
        "AND Billed_Product_Id = '"+billedProductId+"' "+
        "AND Eff_From_Date <= '"+mthlyBillStartDate+"' "+
        "AND ( Eff_To_Date >= '"+mthlyBillStartDate+"' OR Eff_To_Date IS NULL )";
      st = conn.createStatement();
      rs = st.executeQuery(SQL);
    }
    catch(java.sql.SQLException ex)
    {
      System.out.println(SQL);
      message=ex.getMessage();
      System.out.println(message);
    }
    finally
    {
      return rs;
    }
  }
  
  public int getNextGSRSeqNo(String GSR)
  {
    ResultSet rs = null;
    int result = -9999;
    try
    {
      SQL = "EXEC GCD..Get_Next_GSR_Seq_No '"+GSR+"'";
      st = conn.createStatement();
      rs = st.executeQuery(SQL);
      if (rs.next())
        result = rs.getInt(1);
      st.close();
      rs = null;
      }
      catch(java.sql.SQLException ex)
      {
          System.out.println(SQL);
          message=ex.getMessage();
          System.out.println(message);
      }
    finally
    {
      return result;
    }
  }
  
  public int getMarkUpCount()
  {
    ResultSet rs = null;
    int result = -9999;
    try
    {
      SQL = "EXEC GCD..Get_Mark_Up_Count";
      st = conn.createStatement();
      rs = st.executeQuery(SQL);
      if (rs.next())
        result = rs.getInt(1);
      st.close();
      rs = null;
      }
      catch(java.sql.SQLException ex)
      {
          System.out.println(SQL);
          message=ex.getMessage();
          System.out.println(message);
      }
    finally
    {
      return result;
    }
  }
  
  public int getSiteAccounts()
  {
    ResultSet rs = null;
    int result = -9999;
    try
    {
      SQL = "EXEC GCD..Get_Site_Accounts";
      st = conn.createStatement();
      rs = st.executeQuery(SQL);
      if (rs.next())
        result = rs.getInt(1);
      st.close();
      rs = null;
      }
      catch(java.sql.SQLException ex)
      {
          System.out.println(SQL);
          message=ex.getMessage();
          System.out.println(message);
      }
    finally
    {
      return result;
    }
  }
  
  public int validateSiteAccounts()
  {
    ResultSet rs = null;
    int result = -9999;
    try
    {
      SQL = "EXEC GCD..Validate_Site_Accounts";
      st = conn.createStatement();
      rs = st.executeQuery(SQL);
      if (rs.next())
        result = rs.getInt(1);
      st.close();
      rs = null;
      }
      catch(java.sql.SQLException ex)
      {
          System.out.println(SQL);
          message=ex.getMessage();
          System.out.println(message);
      }
    finally
    {
      return result;
    }
  }
  
  public int summariseNCRCharges()
  {
    ResultSet rs = null;
    int result = -9999;
    try
    {
      SQL = "EXEC GCD..Summarise_NCR_Charges";
      st = conn.createStatement();
      rs = st.executeQuery(SQL);
      if (rs.next())
        result = rs.getInt(1);
      st.close();
      rs = null;
      }
      catch(java.sql.SQLException ex)
      {
          System.out.println(SQL);
          message=ex.getMessage();
          System.out.println(message);
      }
    finally
    {
      return result;
    }
  }
  
  public int identifyMissingGSRs()
  {
    ResultSet rs = null;
    int result = -9999;
    try
    {
      SQL = "EXEC GCD..Identify_Missing_GSRs";
      st = conn.createStatement();
      rs = st.executeQuery(SQL);
      if (rs.next())
        result = rs.getInt(1);
      st.close();
      rs = null;
      }
      catch(java.sql.SQLException ex)
      {
          System.out.println(SQL);
          message=ex.getMessage();
          System.out.println(message);
      }
    finally
    {
      return result;
    }
  }
  
  public int updatePPCCodes()
  {
    ResultSet rs = null;
    int result = -9999;
    try
    {
      SQL = "EXEC GCD..Update_PPC_Code";
      st = conn.createStatement();
      rs = st.executeQuery(SQL);
      if (rs.next())
        result = rs.getInt(1);
      st.close();
      rs = null;
      }
      catch(java.sql.SQLException ex)
      {
          System.out.println(SQL);
          message=ex.getMessage();
          System.out.println(message);
      }
    finally
    {
      return result;
    }
  }
  
  public int markupNCRCharge()
  {
    ResultSet rs = null;
    int result = -9999;
    try
    {
      SQL = "EXEC GCD..Markup_NCR_Charge";
      st = conn.createStatement();
      rs = st.executeQuery(SQL);
      if (rs.next())
        result = rs.getInt(1);
      st.close();
      rs = null;
      }
      catch(java.sql.SQLException ex)
      {
          System.out.println(SQL);
          message=ex.getMessage();
          System.out.println(message);
      }
    finally
    {
      return result;
    }
  }
   
  public boolean insertNCRFile(
    String GSR,
    int fileSeqNo,
    String creationDate)
  {
    boolean result = true;
    try
    {
      SQL = "EXEC gcd..Insert_NCR_File '"+
              GSR+"', "+
              fileSeqNo+", '"+
              creationDate+"'";
      st = conn.createStatement();
      st.execute(SQL);
      st.close();
      }
      catch(java.sql.SQLException ex)
      {
          
          System.out.println(SQL);
          message=ex.getMessage();
          System.out.println(message);
          result = false;
      }
    finally
    {
      return result;
    }
  }
  
  public void insertNCRProcessingLog(
    String process,
    String processingMessage )
  {
    try
    {
      SQL = "EXEC gcd..Insert_NCR_Processing_Log '"+
              process+"', '"+
              processingMessage+"'";
      st = conn.createStatement();
      st.execute(SQL);
      st.close();
    }
    catch(java.sql.SQLException ex)
    {

      System.out.println(SQL);
      message=ex.getMessage();
      System.out.println(message);
    }
  }
   
  public boolean insertNCRCharge(
    String GSR,
    int fileSeqNo,
    int recordSeqNo,
    String lineItemDescription,
    String lineItemReference,
    String SN,
    String startDate,
    String startTime,
    String endDate,
    String endTime,
    String siteId,
    String NCRReference,
    String chargeCurrency,
    String chargeAmount,
    String chargeType,
    String chargeDescription )
  {
    boolean result = true;
    try
    {
      SQL = "EXEC gcd..Insert_NCR_Charge '"+
              GSR+"', "+
              fileSeqNo+", '"+
              recordSeqNo+"','"+
              lineItemDescription+"','"+
              lineItemReference+"','"+
              SN+"','"+
              startDate+"','"+
              startTime+"','"+
              endDate+"','"+
              endTime+"','"+
              siteId+"','"+
              NCRReference+"','"+
              chargeCurrency+"','"+
              chargeAmount+"','"+
              chargeType+"','"+
              chargeDescription+"'";
      st = conn.createStatement();
      st.execute(SQL);
      st.close();
      }
      catch(java.sql.SQLException ex)
      {
          
          System.out.println(SQL);
          message=ex.getMessage();
          System.out.println(message);
          result = false;
      }
    finally
    {
      return result;
    }
  }
   
  public boolean updateNCRFile(
    String GSR,
    int fileSeqNo,
    int fileCount)
  {
    boolean result = true;
    try
    {
      SQL = "EXEC gcd..Update_NCR_File '"+
              GSR+"', "+
              fileSeqNo+", "+
              fileCount;
      st = conn.createStatement();
      st.execute(SQL);
      st.close();
      }
      catch(java.sql.SQLException ex)
      {
          
          System.out.println(SQL);
          message=ex.getMessage();
          System.out.println(message);
          result = false;
      }
    finally
    {
      return result;
    }
  }
   
  public boolean deleteNCRFile(
    String GSR,
    int fileSeqNo)
  {
    boolean result = true;
    try
    {
      SQL = "EXEC gcd..Delete_NCR_File '"+
              GSR+"', "+
              fileSeqNo;
      st = conn.createStatement();
      st.execute(SQL);
      st.close();
      }
      catch(java.sql.SQLException ex)
      {
          
          System.out.println(SQL);
          message=ex.getMessage();
          System.out.println(message);
          result = false;
      }
    finally
    {
      return result;
    }
  }
  
  public int getNextSeqNo(
    String genesisCustomer,
    String servicePartner,
    String country,
    String profile,
    String productIdentifier)
  {
    ResultSet rs = null;
    int result = -999;
    try
    {
      SQL = "SELECT Pre_Process_Sequence_No + 1 "+
            "FROM Genesis_File_Sequence (nolock) "+
            "WHERE Genesis_Customer = '"+genesisCustomer+"' "+
            "AND Service_Partner = '"+servicePartner+"' "+
            "AND Country = '"+country+"' "+
            "AND Profile = '"+profile+"' " +
            "AND Product_Identifier = '"+productIdentifier+"' " ;
      st = conn.createStatement();
      rs = st.executeQuery(SQL);
      if (rs.next())
        result = rs.getInt(1);
      st.close();
      rs = null;
      }
      catch(java.sql.SQLException ex)
      {
          System.out.println(SQL);
          message=ex.getMessage();
          System.out.println(message);
      }
    finally
    {
      return result;
    }
  }
  
  public String getExpectedSheetName(
    String genesisCustomer,
    String country,
    String servicePartner)
  {
    ResultSet rs = null;
    String result = "ERROR";
    try
    {
      SQL = "EXEC oss..Expected_Pass_Through_Month "+
            "'"+genesisCustomer+"', "+
            "'"+country+"', "+
            "'"+servicePartner+"'";
      st = conn.createStatement();
      //System.out.println(SQL);
      rs = st.executeQuery(SQL);
      if (rs.next())
        result = rs.getString(1);
      st.close();
      rs = null;
      }
      catch(java.sql.SQLException ex)
      {
          System.out.println(SQL);
          message=ex.getMessage();
          System.out.println(message);
      }
    finally
    {
      return result;
    }
  }
  
  public boolean updateSeqNo(
    String genesisCustomer,
    String servicePartner,
    String country,
    String profile,
    String productIdentifier,
    int seqNo)
  {
    ResultSet rs = null;
    boolean result = true;
    try
    {
      SQL = "UPDATE Genesis_File_Sequence "+
            "SET Pre_Process_Sequence_No = "+seqNo+ " "+
            "WHERE Genesis_Customer = '"+genesisCustomer+"' "+
            "AND Service_Partner = '"+servicePartner+"' "+
            "AND Country = '"+country+"' "+
            "AND Profile = '"+profile+"' " +
            "AND Product_Identifier = '"+productIdentifier+"' " ;
      st = conn.createStatement();
      st.execute(SQL);
      st.close();
      rs = null;
      }
      catch(java.sql.SQLException ex)
      {
          System.out.println(SQL);
          message=ex.getMessage();
          System.out.println(message);
          result = false;
      }
    finally
    {
      return result;
    }
  }

  // get result set of job queue entries
  public long queueCount()
  {
    ResultSet rs = null;
    long result = 0;
    try
    {
      SQL = "SELECT COUNT(*) FROM Job_Queue (nolock)";
      st = conn.createStatement();
      rs = st.executeQuery(SQL);
      if (rs.next())
      {
        result = rs.getLong(1);
      }
      st.close();
    }
    catch(java.sql.SQLException ex)
    {
      System.out.println(SQL);
      message=ex.getMessage();
      System.out.println(message);
    }
    finally
    {
      return result;
    }
  }

  // check if local data item exists
  public boolean localDataExists
    (String conglomCustId, String billedProductId, String reportType, String sourceAccountNo, String billedNumber)
  {
    ResultSet rs = null;
    boolean result = false;
    try
    {
      SQL =
        "SELECT COUNT(*) LD_Count "+
        "FROM Source_Acct_Local_Data (nolock) "+
        "WHERE Conglom_Cust_Id = "+conglomCustId+" "+
        "AND Billed_Product_Id = '"+billedProductId+"' "+
        "AND Report_Type = '"+reportType+"' "+
        "AND Source_Account_No = '"+sourceAccountNo+"' "+
        "AND Billed_Number = '"+billedNumber+"'";
      st = conn.createStatement();
      rs = st.executeQuery(SQL);
      if (rs.next())
      {
        int ldCount = rs.getInt("LD_Count");
        if (ldCount>0)
          result = true;
      }
      st.close();
    }
    catch(java.sql.SQLException ex)
    {
      System.out.println(SQL);
      message=ex.getMessage();
      System.out.println(message);
    }
    finally
    {
      return result;
    }
  }

  // insert new local data item
  public boolean insertLocalData
    (String conglomCustId, String billedProductId, String reportType, String sourceAccountNo, String billedNumber,
     String ldDataItem1, String ldSortItem1, String lastUpdateId)
  {
    boolean result = false;
    try
    {
      SQL =
        "INSERT INTO Source_Acct_Local_Data "+
        "(Conglom_Cust_Id,Billed_Product_Id,Report_Type,Source_Account_No,Billed_Number,"+
        "LD_Data_Item1,LD_Sort_Item1,Last_Update_Id) "+
        "VALUES("+conglomCustId+",'"+billedProductId+"','"+reportType+"','"+sourceAccountNo+"','"+billedNumber+"','"+
        ldDataItem1+"','"+ldSortItem1+"','"+lastUpdateId+"')";
      st = conn.createStatement();
      st.execute(SQL);
      st.close();
      result = true;
    }
    catch(java.sql.SQLException ex)
    {
      System.out.println(SQL);
      message=ex.getMessage();
      System.out.println(message);
    }
    finally
    {
      return result;
    }
  }

  // update existing local data item
  public boolean updateLocalData
    (String conglomCustId, String billedProductId, String reportType, String sourceAccountNo, String billedNumber,
     String ldDataItem, String ldSortItem, int seqNo, String lastUpdateId)
  {
    boolean result = false;
    try
    {
      SQL =
        "UPDATE Source_Acct_Local_Data "+
        "SET LD_Data_Item"+seqNo+" = '"+ldDataItem+"' ";
      if (seqNo<4)
        SQL = SQL + ", LD_Sort_Item"+seqNo+" = '"+ldSortItem+"' ";
      SQL = SQL +
        ", Last_Update_Date = GETDATE(), Last_Update_Id = '"+lastUpdateId+"' "+
        "WHERE Conglom_Cust_Id = "+conglomCustId+" "+
        "AND Billed_Product_Id = '"+billedProductId+"' "+
        "AND Report_Type = '"+reportType+"' "+
        "AND Source_Account_No = '"+sourceAccountNo+"' "+
        "AND Billed_Number = '"+billedNumber+"'";
      st = conn.createStatement();
      st.execute(SQL);
      st.close();
      result = true;
    }
    catch(java.sql.SQLException ex)
    {
      System.out.println(SQL);
      message=ex.getMessage();
      System.out.println(message);
    }
    finally
    {
      return result;
    }
  }

  public boolean copyJobFromQueue()
  {
    ResultSet rs = null;
    boolean result = false;
    try
    {
      SQL = "exec Copy_Job_From_Queue 'Goldfish Extract'";
      st = conn.createStatement();
      rs = st.executeQuery(SQL);
      st.close();
      result = true;
    }
    catch(java.sql.SQLException ex)
    {
      System.out.println(SQL);
      message=ex.getMessage();
      System.out.println(message);
    }
    finally
    {
      rs = null;
      return result;
    }
  }

  public boolean resetBPRC(String conglomCustId, String billedProductId)
  {
    boolean result = false;
    try
    {
      SQL =
        "UPDATE Billing_Period_Run_Control "+
        "SET    Active_Ind = 'N' "+
        "WHERE  Conglom_Cust_Id = "+conglomCustId+" "+
        "AND    Billed_Product_Id = '"+billedProductId+"'";
      st = conn.createStatement();
      st.execute(SQL);
      result = true;
      st.close();
    }
    catch(java.sql.SQLException ex)
    {
      System.out.println(SQL);
      message=ex.getMessage();
      System.out.println(message);
    }
    finally
    {
      rs = null;
      return result;
    }
  }

  // get result set of job queue entries
  public ResultSet getJobDetails()
  {
    ResultSet rs = null;
    try
    {
      SQL =
        "SELECT cc.Conglom_Cust_Id, "+
        "convert(varchar(23),bprc.Billing_Period_Start_Date,121) Billing_Period_Start_Date, "+
        "convert(varchar(23),bprc.Billing_Period_End_Date,121) Billing_Period_End_Date, "+
        "bprc.Billed_Product_Id, "+
        "bprc.Bill_Period_Ref, "+
        "bprc.Billing_Period_RefMMYY, "+
        "bprc.Extract_Type, "+
        "bp.Source_System_Id, "+
        "ccp.Source_Conglom_Id, "+
        "bprc.Month_Closed_Ind, "+
        "bprc.Last_Update_Id, "+
        "cc.Vat_Exempt_Ind, "+
        "convert(varchar(23),convert(datetime,BPRC.Bill_Period_Ref+'01',112),121)  Mthly_Bill_Start_Date, "+
        "convert(varchar(23),dateadd(dd,-1,dateadd(mm,1,convert(datetime,BPRC.Bill_Period_Ref+'01',112))),121) Mthly_Bill_End_Date, "+
        "convert(varchar(23),convert(datetime,BPRC.Bill_Period_Ref+'01',112),121)  Qtrly_Bill_Start_Date, "+
        "convert(varchar(23),dateadd(dd,-1,dateadd(mm,3,convert(datetime,BPRC.Bill_Period_Ref+'01',112))),121) Qtrly_Bill_End_Date "+
        "FROM Billing_Period_Run_Control bprc (nolock), "+
        "Conglom_Customer cc (nolock), "+
        "Conglom_Cust_Product ccp (nolock), "+
        "Billed_Product bp (nolock) "+
        "WHERE cc.Conglom_Cust_Id = bprc.Conglom_Cust_Id "+
        "AND bprc.Billed_Product_Id = bp.Billed_Product_Id "+
        "AND ccp.Conglom_Cust_Id = bprc.Conglom_Cust_Id "+
        "AND ccp.Billed_Product_Id = bprc.Billed_Product_Id "+
        "AND bprc.Active_Ind = 'Y' "+
        "AND bprc.Jobname = 'Goldfish Extract'";
      st = conn.createStatement();
      rs = st.executeQuery(SQL);
    }
    catch(java.sql.SQLException ex)
    {
      System.out.println(SQL);
      message=ex.getMessage();
      System.out.println(message);
    }
    finally
    {
      return rs;
    }
  }
  
  // get all Report Requests where status is Requested
  public ResultSet getRequestedReportRequests()
  {
      ResultSet rs = null;
      try
      {
          SQL = "SELECT * FROM Report_Request WHERE Request_Status = 'Requested'";
          st = conn.createStatement();
          rs = st.executeQuery(SQL);;
      }
      catch(java.sql.SQLException ex)
      {
          System.out.print(SQL);
          message = ex.getMessage();
          System.out.println(message);
      }
      finally
      {
          return rs;
      }
  }
  
  // get all Report Requests where status is In Progress  
  public ResultSet getInProgressReportRequests()
  {
      ResultSet rs = null;
      try
      {
          SQL = 
              "SELECT Global_Customer_Id,"+
              "Invoice_No,"+
              "Account_Number,"+
              "Account_Name,"+
              "Billing_Source,"+
              "ISO_Currency_Code,"+
              "Tax_Point_Date = CONVERT(VARCHAR(11),Tax_Point_Date,113),"+
              "Period_From_Date = CONVERT(VARCHAR(11),Period_From_Date,113),"+
              "Period_To_Date = CONVERT(VARCHAR(11),Period_To_Date,113),"+
              "Invoice_Total, "+
              "Trial_Ind, "+
              "Report_Name, "+
              "Display_Name, "+
              "Report_Type, " +
              "Report_Filename, "+
              "Report_Request_Id, " +
              "Printer = ISNULL( Printer, '' ) " +
              "FROM Report_Request "+
              "WHERE Request_Status = 'In Progress' "+
              "ORDER BY Report_Filename";
          st = conn.createStatement();
          rs = st.executeQuery(SQL);;
      }
      catch(java.sql.SQLException ex)
      {
          System.out.print(SQL);
          message = ex.getMessage();
          System.out.println(message);
      }
      finally
      {
          return rs;
      }
  }
  
  // get all Report Requests where status is In Progress  
  public ResultSet getArchiveReportRequests(String archivePeriod)
  {
      ResultSet rs = null;
      try
      {
          SQL = 
              "SELECT * "+
              "FROM Report_Request "+
              "WHERE Request_Status = 'Completed' "+
              "AND Trial_Ind = 'Y' "+
              "AND DATEDIFF( d, Last_Update_Date, GETDATE() ) > "+archivePeriod+" ";
          st = conn.createStatement();
          rs = st.executeQuery(SQL);;
      }
      catch(java.sql.SQLException ex)
      {
          System.out.print(SQL);
          message = ex.getMessage();
          System.out.println(message);
      }
      finally
      {
          return rs;
      }
  }
  
   // update selected report requests to In Progress
   public boolean updateInProgressReportRequests
    (String updateINStatement)
  {
    boolean result = false;
    try
    {
      SQL =
        "UPDATE Report_Request "+
        "SET Request_Status = 'In Progress', " +
        "Last_Update_Date = GETDATE(), Last_Update_Id = 'processInitialReportRequests' " +
        "WHERE Report_Request_Id IN "+ updateINStatement;
      st=conn.createStatement();
      st.execute(SQL);
      st.close();
      result = true;
    }
    catch(java.sql.SQLException ex)
    {
      System.out.println(SQL);
      message=ex.getMessage();
      System.out.println(message);
    }
    return result;
  }
   
   // update reports return requests that have been archive
   public boolean updateReportRequestForArchive(String reportRequestId)
   {
       boolean result = false;
       SQL = 
        "UPDATE Report_Request "+
        "SET Request_Status = 'Archived', "+
        "Last_Update_Date = GETDATE(), Last_Update_Id = 'archiveTrialReportRequests' " +
        "WHERE Report_Request_Id = " + reportRequestId;
       try
       {
           st=conn.createStatement();
           st.execute(SQL);
           st.close();
           result = true;
       }
       catch(java.sql.SQLException ex)
       {
           System.out.println(SQL);
           message=ex.getMessage();
           System.out.println(message);
       }
       return result;
   }
   
    // update In Progress report requests
    public boolean updateInProgressReportRequest
        (long reportRequestId,
         String failedInd,
         String failureMessage,
         String trialInd)
    {
        boolean result = false;
        // build relevant SQL
        SQL = "UPDATE Report_Request ";
        if (failedInd.startsWith("Y"))
            SQL = SQL + 
                "SET Request_Status = 'Failed', "+
                " Failure_Message = '" + failureMessage + "', ";
        else
            SQL = SQL + "SET Request_Status = 'Completed', ";
        SQL = SQL +
            "Last_Update_Date = GETDATE(), Last_Update_Id = 'updateInProgressReportRequests'";
        if ((trialInd.startsWith("N"))&&(failedInd.startsWith("N")))
            SQL = SQL +
            ", Transfer_Ind = 'Y' ";
            
        else
            SQL = SQL + " ";
        SQL = SQL + "WHERE Report_Request_Id = " + reportRequestId;
        try
        {
          st=conn.createStatement();
          st.execute(SQL);
          st.close();
          result = true;
        }
        catch(java.sql.SQLException ex)
        {
          System.out.println(SQL);
          message=ex.getMessage();
          System.out.println(message);
        }
        return result;
    }
  
  // get result set of job queue entries
  public ResultSet getLocalDataDataItems(String conglomCustId, String billedProductId)
  {
    ResultSet rs = null;
    try
    {
      SQL =
        "SELECT Report_Type, "+
        "LD_Ref_Type, "+
        "LD_Column_Seq_No, "+
        "LD_Sort_Seq_No "+
        "FROM Cust_Local_Data_Ref_Item (nolock) "+
        "WHERE Conglom_Cust_Id = '"+conglomCustId+"' "+
        "AND Billed_Product_Id =  '"+billedProductId+"' "+
        "ORDER BY Report_Type, LD_Column_Seq_No";
      st = conn.createStatement();
      rs = st.executeQuery(SQL);
    }
    catch(java.sql.SQLException ex)
    {
      System.out.println(SQL);
      message=ex.getMessage();
      System.out.println(message);
    }
    finally
    {
      return rs;
    }
  }

  public void writeLogMessage
    (String conglomCustId,
     String billingPeriodStartDate,
     String jobId,
     String messageTime,
     String messageType,
     String message )
  {
    try
    {
      String finalMessage = "";
      for (int i=0; i<message.length(); i++)
      {
        String test = message.substring(i,i+1);
        if (i<255)
          if (!test.equals("\'"))
            finalMessage = finalMessage+test;
          else
            finalMessage = finalMessage+"|";
      }
      SQL =
        "INSERT INTO Billing_Log "+
        "(Conglom_Cust_Id, Billing_Period_Start_Date, Message_Type, Job_Id,"+
        " Message_Time, Jobname, Message ) "+
        "VALUES( "+conglomCustId+",'"+billingPeriodStartDate+"','"+messageType+"','"+
        jobId+"','"+messageTime+"','Goldfish Extract','"+finalMessage+"')";
      st=conn.createStatement();
      st.execute(SQL);
      st.close();
    }
    catch(java.sql.SQLException ex)
    {
      System.out.println(SQL);
      message=ex.getMessage();
      System.out.println(message);
    }
  }

  public String getExceptionStatus
    (String exceptionType )
  {
    String result="Unknown";
    try
    {
      ResultSet rs = null;
      SQL =
        "SELECT Default_Status "+
        "FROM Exception_Type (nolock) "+
        "WHERE Exception_Type = '"+exceptionType+"'";
      st = conn.createStatement();
      rs = st.executeQuery(SQL);
      if (rs.next())
        result = rs.getString(1);
      rs=null;
      st.close();
    }
    catch(java.sql.SQLException ex)
    {
      System.out.println(SQL);
      message=ex.getMessage();
      System.out.println(message);
    }
    finally
    {
      rs = null;
      return result;
    }
  }

  public boolean excludedAccount (String conglomCustId, String billedProductId, String discountItemCode, String sourceAccountNo )
  {
    boolean result = false;
    try
    {
      ResultSet rs = null;
      SQL =
        "SELECT COUNT(*) "+
        "FROM Conglom_Disc_Excluded_Acct (nolock) "+
        "WHERE Conglom_Cust_Id = "+conglomCustId+" "+
        "AND Billed_Product_Id = '"+billedProductId+"' "+
        "AND Discount_Item_Code = '"+discountItemCode+"' "+
        "AND Source_Account_No = '"+sourceAccountNo+"'";
      st = conn.createStatement();
      rs = st.executeQuery(SQL);
      if (rs.next())
      {
        int count = rs.getInt(1);
        if (count>0)
          result = true;
      }
      st.close();
    }
    catch(java.sql.SQLException ex)
    {
      System.out.println(SQL);
      message=ex.getMessage();
      System.out.println(message);
    }
    finally
    {
      rs = null;
      return result;
    }
  }

  public void writeTempException
    (String conglomCustId,
     String billedProductId,
     String billPeriodRef,
     String runReportType,
     String sourceAccountNo,
     String docketInvoice,
     String dateBilled,
     long amount1,
     long amount2,
     String status,
     String exceptionType,
     String lastUpdateId)
  {
    try
    {
      SQL =
        "INSERT INTO Conglom_Temp_Exception "+
        "(Conglom_Cust_Id, Billed_Product_Id, Bill_Period_Ref, Run_Report_Type,"+
        " Run_Timestamp, Source_Account_No, Docket_Invoice, Date_Billed,"+
        " Amount_1, Amount_2, Status, Exception_Type, Last_Update_Date,"+
        " Last_Update_Id, Period_Ref) "+
        "VALUES( "+conglomCustId+",'"+billedProductId+"','"+billPeriodRef+"','"+runReportType+
        "',GETDATE(),'"+sourceAccountNo+"','"+docketInvoice+"','"+dateBilled+
        "',"+makeLongDecimal(amount1)+","+makeLongDecimal(amount2)+",'"+status+"','"+exceptionType+
        "',GETDATE(),'"+lastUpdateId+"',NULL)";
      st=conn.createStatement();
      st.execute(SQL);
      st.close();
    }
    catch(java.sql.SQLException ex)
    {
      System.out.println(SQL);
      message=ex.getMessage();
      System.out.println(message);
    }
  }

  public void resolveExceptions( String userId, String conglomCustId, String billedProductId )
  {
    try
    {
      SQL = "exec Resolve_Exceptions '"+userId+"',"+conglomCustId+",'"+billedProductId+"','Extract'";
      st = conn.createStatement();
      st.execute(SQL);
      st.close();
    }
    catch(java.sql.SQLException ex)
    {
      System.out.println(SQL);
      message=ex.getMessage();
      System.out.println(message);
    }
  }

  public boolean beginTran()
  {
    boolean result = false;
    try
    {
      SQL = "BEGIN TRAN goldfishExtract";
      st = conn.createStatement();
      st.execute(SQL);
      st.close();
      result = true;
    }
    catch(java.sql.SQLException ex)
    {
      System.out.println(SQL);
      message=ex.getMessage();
      System.out.println(message);
      result = false;
    }
    finally
    {
      return result;
    }
  }

  public boolean commitTran()
  {
    boolean result = false;
    try
    {
      SQL = "SET CURSOR_CLOSE_ON_COMMIT ON";
      st = conn.createStatement();
      st.execute(SQL);
      st.close();
      SQL = "COMMIT TRAN goldfishExtract";
      st2 = conn.createStatement();
      st2.execute(SQL);
      st2.close();
      result = true;
    }
    catch(java.sql.SQLException ex)
    {
      System.out.println(SQL);
      message=ex.getMessage();
      System.out.println(message);
      result = false;
    }
    finally
    {
      return result;
    }
  }

  public boolean rollbackTran()
  {
    boolean result = false;
    try
    {
      SQL = "SET CURSOR_CLOSE_ON_COMMIT ON";
      st = conn.createStatement();
      st.execute(SQL);
      st.close();
      SQL = "ROLLBACK TRAN goldfishExtract";
      st2 = conn.createStatement();
      st2.execute(SQL);
      st2.close();
      result = true;
    }
    catch(java.sql.SQLException ex)
    {
      System.out.println(SQL);
      message=ex.getMessage();
      System.out.println(message);
      result = false;
    }
    finally
    {
      return result;
    }
  }

  public boolean clearSourceBills(String conglomCustId, String billedProductId, String billPeriodRef)
  {
    boolean result = false;
    try
    {
      SQL = "exec clear_source_bills "+conglomCustId+",'"+billedProductId+"','"+billPeriodRef+"'";
      st = conn.createStatement();
      st.execute(SQL);
      st.close();
      result = true;
    }
    catch(java.sql.SQLException ex)
    {
      System.out.println(SQL);
      message=ex.getMessage();
      System.out.println(message);
      result = true;
    }
    finally
    {
      return result;
    }
  }

  public void clearPCBCredits(String conglomCustId, String billedProductId, String billPeriodRef)
  {
    try
    {
      SQL = "exec clear_PCB_credits "+conglomCustId+",'"+billedProductId+"','"+billPeriodRef+"'";
      st = conn.createStatement();
      st.execute(SQL);
      st.close();
    }
    catch(java.sql.SQLException ex)
    {
      System.out.println(SQL);
      message=ex.getMessage();
      System.out.println(message);
    }
  }

  // check if source invoice exists
  public boolean sourceInvoiceExists (String conglomCustId, String billedProductId, String invoiceNumber)
  {
    ResultSet rs = null;
    boolean result = false;
    try
    {
      SQL =
        "SELECT * "+
        "FROM Source_Invoice (nolock) "+
        "WHERE Conglom_Cust_Id = "+conglomCustId+" "+
        "AND   Billed_Product_Id = '"+billedProductId+"' "+
        "AND   Source_Invoice_No = '"+invoiceNumber+"'";
      st = conn.createStatement();
      rs = st.executeQuery(SQL);
      if (rs.next())
        result = true;
      st.close();
    }
    catch(java.sql.SQLException ex)
    {
      System.out.println(SQL);
      message=ex.getMessage();
      System.out.println(message);
    }
    finally
    {
      rs = null;
      return result;
    }
  }
  // check if source adjustment exists
  public boolean sourceAdjustmentExists (String conglomCustId, String billedProductId, String docketNumber)
  {
    ResultSet rs = null;
    boolean result = false;
    try
    {
      SQL =
        "SELECT * "+
        "FROM Source_Adjustment (nolock) "+
        "WHERE Conglom_Cust_Id = "+conglomCustId+" "+
        "AND   Billed_Product_Id = '"+billedProductId+"' "+
        "AND   Docket_Number = '"+docketNumber+"'";
      st = conn.createStatement();
      rs = st.executeQuery(SQL);
      if (rs.next())
        result = true;
      st.close();
    }
    catch(java.sql.SQLException ex)
    {
      System.out.println(SQL);
      message=ex.getMessage();
      System.out.println(message);
    }
    finally
    {
      rs = null;
      return result;
    }
  }

  public String insertOneOffCharge
    (String conglomCustId,
     String billedProductId,
     String billPeriodRef,
     String sourceInvoiceNo,
     String sourceAccountNo,
     String chargeTypeCode,
     String chargeDate,
     String contractNumber,
     String circuitReference,
     String chargeDescription,
     long chargeQuantity,
     long chargeAmount,
     String lastUpdateId )
  {
    String result = "OK";
    try
    {
      SQL =
        "INSERT INTO Source_One_Off_Charge "+
        "(Conglom_Cust_Id, Billed_Product_Id, Bill_Period_Ref, Source_Invoice_No, "+
        "Source_Account_No, Charge_Type_Code, Charge_Date, Contract_Number, Circuit_Reference, "+
        "Charge_Description, Charge_Quantity, Charge_Amount, Last_Update_Id ) "+
        "VALUES("+conglomCustId+",'"+billedProductId+"','"+billPeriodRef+"','"+sourceInvoiceNo+
        "','"+sourceAccountNo+"','"+chargeTypeCode+"','"+chargeDate+"',";
      if (contractNumber.equals("NULL"))
        SQL = SQL + "NULL,";
      else
        SQL = SQL + "'"+contractNumber+"',";
      if (circuitReference.equals("NULL"))
        SQL = SQL + "NULL,'";
      else
        SQL = SQL + "'"+circuitReference+"','";
      SQL = SQL + chargeDescription+"',"+chargeQuantity+","+makeLongDecimal(chargeAmount)+",'"+lastUpdateId+"')";
      st=conn.createStatement();
      st.execute(SQL);
      st.close();
    }
    catch(java.sql.SQLException ex)
    {
      System.out.println(SQL);
      message=ex.getMessage();
      System.out.println(message);
      result = message;
    }
    return result;
  }

  public String insertRecurringCharge
    (String conglomCustId,
     String billedProductId,
     String billPeriodRef,
     String sourceInvoiceNo,
     String sourceAccountNo,
     String chargeTypeCode,
     String periodStartDate,
     String periodEndDate,
     String circuitDescription,
     long quantity,
     long netAmount,
     long vatAmount,
     String lastUpdateId,
     String contractNumber,
     String circuitReference,
     String aEnd,
     String bEnd,
     String circuitLength,
     String vatExemptInd)
  {
    String result = "OK";
    try
    {
      SQL =
        "INSERT INTO Source_Recurring_Charge "+
        "(Conglom_Cust_Id, Billed_Product_Id, Bill_Period_Ref, Source_Invoice_No, "+
        "Source_Account_No, Charge_Type_Code, Period_Start_Date, Period_End_Date, "+
        "Circuit_Description, Quantity, Net_Amount, Last_Update_Id, Contract_Number, "+
        "Circuit_Reference, A_End, B_End, Circuit_Length, VAT_Amount, Total_Amount) "+
        "VALUES("+conglomCustId+",'"+billedProductId+"','"+billPeriodRef+"','"+sourceInvoiceNo+
        "','"+sourceAccountNo+"','"+chargeTypeCode+"','"+periodStartDate+"','"+periodEndDate+
        "','"+circuitDescription+"',"+quantity+","+makeLongDecimal(netAmount)+",'"+lastUpdateId+"',";
      if (contractNumber.equals("NULL"))
        SQL = SQL + "NULL,";
      else
        SQL = SQL + "'"+contractNumber+"',";
      if (circuitReference.equals("NULL"))
        SQL = SQL + "NULL,";
      else
        SQL = SQL + "'"+circuitReference+"',";
      if (aEnd.equals("NULL"))
        SQL =  SQL + "NULL,";
      else
        SQL = SQL + "'"+aEnd+"',";
      if (bEnd.equals("NULL"))
        SQL =  SQL + "NULL,";
      else
        SQL = SQL + "'"+bEnd+"',";
      if (circuitLength.equals("NULL"))
        SQL = SQL + "NULL,";
      else
        SQL = SQL + "'" + circuitLength + "',";
      if (vatExemptInd.equals("Y"))
        SQL = SQL+"0,"+netAmount+")";
      else
        SQL = SQL +
          makeLongDecimal3(vatAmount)+","+makeLongDecimal3((netAmount*10)+vatAmount)+")";
      st=conn.createStatement();
      st.execute(SQL);
      st.close();
    }
    catch(java.sql.SQLException ex)
    {
      System.out.println(SQL);
      message=ex.getMessage();
      System.out.println(message);
      result = message;
    }
    return result;
  }

  public String insertCallinkCharge
    (String conglomCustId,
     String billedProductId,
     String billPeriodRef,
     String sourceInvoiceNo,
     String sourceAccountNo,
     String chargeTypeCode,
     long chargeId,
     String chargeDescription,
     long chargeAmount,
     String lastUpdateId,
     String chargeStartDate,
     String chargeEndDate)
  {
    String result = "OK";
    try
    {
      SQL =
        "INSERT INTO Source_Usage_Other_Charge "+
        "(Conglom_Cust_Id, Billed_Product_Id, Bill_Period_Ref, Source_Invoice_No, "+
        "Source_Account_No, Charge_Type_Code, Charge_Id, Charge_Description, "+
        "Charge_Amount, Last_Update_Id, Charge_Start_Date, Charge_End_Date) "+
        "VALUES("+conglomCustId+",'"+billedProductId+"','"+billPeriodRef+"','"+sourceInvoiceNo+
        "','"+sourceAccountNo+"','"+chargeTypeCode+"',"+chargeId+",'"+chargeDescription+
        "',"+makeLongDecimal(chargeAmount)+",'"+lastUpdateId+"','"+chargeStartDate+"','"+chargeEndDate+"')";
      st=conn.createStatement();
      st.execute(SQL);
      st.close();
    }
    catch(java.sql.SQLException ex)
    {
      System.out.println(SQL);
      message=ex.getMessage();
      System.out.println(message);
      result = message;
    }
    return result;
  }

  public String insertSundryCharge
    (String conglomCustId,
     String billedProductId,
     String billPeriodRef,
     String sourceInvoiceNo,
     String sourceAccountNo,
     String chargeTypeCode,
     String sundryCode,
     String description,
     long netAmount,
     String lastUpdateId)
  {
    String result = "OK";
    try
    {
      SQL =
        "INSERT INTO Source_Sundry_Charge "+
        "(Conglom_Cust_Id, Billed_Product_Id, Bill_Period_Ref, Source_Invoice_No, "+
        "Source_Account_No, Charge_Type_Code, Sundry_Code, Description, "+
        "Net_Amount, Last_Update_Id) "+
        "VALUES("+conglomCustId+",'"+billedProductId+"','"+billPeriodRef+"','"+sourceInvoiceNo+
        "','"+sourceAccountNo+"','"+chargeTypeCode;
      if(sundryCode.equals("NULL"))
        SQL = SQL + "',NULL,'";
      else
        SQL = SQL+"','"+sundryCode+"','";
      SQL = SQL+
        description+
        "',"+makeLongDecimal(netAmount)+",'"+lastUpdateId+"')";
      st=conn.createStatement();
      st.execute(SQL);
      st.close();
    }
    catch(java.sql.SQLException ex)
    {
      System.out.println(SQL);
      message=ex.getMessage();
      System.out.println(message);
      result = message;
    }
    return result;
  }

  public String insertVPNCharge
    (String conglomCustId,
     String billedProductId,
     String billPeriodRef,
     String sourceInvoiceNo,
     String sourceAccountNo,
     String chargeTypeCode,
     String currentPeriodStartDate,
     String currentPeriodEndDate,
     long currentPeriodUsageTotal,
     String lastUpdateId)
  {
    String result = "OK";
    try
    {
      SQL =
        "INSERT INTO Source_Usage_Summary "+
        "(Conglom_Cust_Id, Billed_Product_Id, Bill_Period_Ref, Source_Invoice_No, "+
        "Source_Account_No, Charge_Type_Code, Curr_Period_Start_Date, "+
        "Curr_Period_End_Date, Total_Usage_Charges, Last_Update_Id, "+
        "Curr_Period_Usage_Total, Prev_Period_Usage_Total) "+
        "VALUES("+conglomCustId+",'"+billedProductId+"','"+billPeriodRef+"','"+sourceInvoiceNo+
        "','"+sourceAccountNo+"','"+chargeTypeCode+"','"+currentPeriodStartDate+"','"+
        currentPeriodEndDate+"',"+makeLongDecimal(currentPeriodUsageTotal)+",'"+lastUpdateId+"',"+
        makeLongDecimal(currentPeriodUsageTotal)+",0)";
      st=conn.createStatement();
      st.execute(SQL);
      st.close();
    }
    catch(java.sql.SQLException ex)
    {
      System.out.println(SQL);
      message=ex.getMessage();
      System.out.println(message);
      result = message;
    }
    return result;
  }

  public String insertCallCharge
    (String conglomCustId,
     String billedProductId,
     String billPeriodRef,
     String sourceInvoiceNo,
     String sourceAccountNo,
     String chargeTypeCode,
     String currentPeriodStartDate,
     String currentPeriodEndDate,
     String previousPeriodStartDate,
     String previousPeriodEndDate,
     long currentPeriodUsageTotal,
     long previousPeriodUsageTotal,
     String lastUpdateId)
  {
    String result = "OK";
    try
    {
      long usageTotal =currentPeriodUsageTotal + previousPeriodUsageTotal;
      SQL =
        "INSERT INTO Source_Usage_Summary "+
        "(Conglom_Cust_Id, Billed_Product_Id, Bill_Period_Ref, Source_Invoice_No, "+
        "Source_Account_No, Charge_Type_Code, Curr_Period_Start_Date, Curr_Period_End_Date, "+
        "Prev_Period_Start_Date, Prev_Period_End_Date, Total_Usage_Charges, Last_Update_Id, "+
        "Curr_Period_Usage_Total, Prev_Period_Usage_Total) "+
        "VALUES("+conglomCustId+",'"+billedProductId+"','"+billPeriodRef+"','"+sourceInvoiceNo+
        "','"+sourceAccountNo+"','"+chargeTypeCode+"','"+currentPeriodStartDate+"','"+currentPeriodEndDate+
        "','"+previousPeriodStartDate+"','"+previousPeriodEndDate+"',"+makeLongDecimal(usageTotal)+",'"+lastUpdateId+"',"+
        makeLongDecimal(currentPeriodUsageTotal)+","+makeLongDecimal(previousPeriodUsageTotal)+")";
      st=conn.createStatement();
      st.execute(SQL);
      st.close();
    }
    catch(java.sql.SQLException ex)
    {
      System.out.println(SQL);
      message=ex.getMessage();
      System.out.println(message);
      result = message;
    }
    return result;
  }

  public String insertEasyAccessQtrlyCharge
    (String conglomCustId,
     String billedProductId,
     String billPeriodRef,
     String sourceInvoiceNo,
     String sourceAccountNo,
     String qtrlyChargeDate,
     long qtrlyCharge,
     String qtrlyDiscountDate,
     long qtrlyDiscountAmount,
     String lastUpdateId)
  {
    String result = "OK";
    try
    {
      SQL =
        "INSERT INTO Source_EasyAcc_Qtrly_Charge "+
        "(Conglom_Cust_Id, Billed_Product_Id, Bill_Period_Ref, Source_Invoice_No, "+
        "Source_Account_No, Qtrly_Charge_Date, Qtrly_Charge, "+
        "Qtrly_Discount_Date, Qtrly_Discount_Amount, Last_Update_Id) "+
        "VALUES("+conglomCustId+",'"+billedProductId+"','"+billPeriodRef+"','"+sourceInvoiceNo+
        "','"+sourceAccountNo+"','"+qtrlyChargeDate+"',"+makeLongDecimal(qtrlyCharge)+
        ",'"+qtrlyDiscountDate+"',"+makeLongDecimal(qtrlyDiscountAmount)+",'"+lastUpdateId+"')";
      st=conn.createStatement();
      st.execute(SQL);
      st.close();
    }
    catch(java.sql.SQLException ex)
    {
      System.out.println(SQL);
      message=ex.getMessage();
      System.out.println(message);
      result = message;
    }
    return result;
  }

  public String insertEasyAccessUsageCharge
    (String conglomCustId,
     String billedProductId,
     String billPeriodRef,
     String sourceInvoiceNo,
     String sourceAccountNo,
     String chargeTypeCode,
     long previousPeriodAmount,
     String previousPeriodStartDate,
     String previousPeriodEndDate,
     long currentPeriodAmount,
     String currentPeriodStartDate,
     String currentPeriodEndDate,
     String lastUpdateId)
  {
    String result = "OK";
    try
    {
      SQL =
        "INSERT INTO Source_Usage_Summary "+
        "(Conglom_Cust_Id, Billed_Product_Id, Bill_Period_Ref, Source_Invoice_No, "+
        "Source_Account_No, Charge_Type_Code, Curr_Period_Start_Date, Curr_Period_End_Date, "+
        "Prev_Period_Start_Date, Prev_Period_End_Date, Total_Usage_Charges, "+
        "Last_Update_Id, Curr_Period_Usage_Total, Prev_Period_Usage_Total) "+
        "VALUES("+conglomCustId+",'"+billedProductId+"','"+billPeriodRef+"','"+sourceInvoiceNo+
        "','"+sourceAccountNo+"','"+chargeTypeCode+"','"+currentPeriodStartDate+"','"+currentPeriodEndDate+
        "','"+previousPeriodStartDate+"','"+previousPeriodEndDate+"',"+makeLongDecimal(currentPeriodAmount+previousPeriodAmount)+
        ",'"+lastUpdateId+"',"+makeLongDecimal(currentPeriodAmount)+","+makeLongDecimal(previousPeriodAmount)+")";
      st=conn.createStatement();
      st.execute(SQL);
      st.close();
    }
    catch(java.sql.SQLException ex)
    {
      System.out.println(SQL);
      message=ex.getMessage();
      System.out.println(message);
      result = message;
    }
    return result;
  }

  public String insertBillingNumberCharge
    (String conglomCustId,
     String billedProductId,
     String billPeriodRef,
     String sourceInvoiceNo,
     String sourceAccountNo,
     String billingNumber,
     String gvpnInd,
     String costCentre,
     long durationMinutes,
     long durationSeconds,
     long numberOfCalls,
     long billNumberCharge,
     long discountCharges,
     long sourceDiscountTotal,
     String lastUpdateId)
  {
    String result = "OK";
    try
    {
      SQL =
        "INSERT INTO Source_Bill_Number_Summary "+
        "(Conglom_Cust_Id, Billed_Product_Id, Bill_Period_Ref, Source_Invoice_No, "+
        "Source_Account_No, Billing_Number, Bill_Number_Expanded, GVPN_Ind, "+
        "Cost_Centre, Duration_Minutes, Duration_Seconds, Number_Of_Calls, "+
        "Bill_Number_Charge, "+
        "Bill_Number_Discount, "+
        "Bill_Number_Total, "+
        "Last_Update_Id) "+
        "VALUES("+conglomCustId+",'"+billedProductId+"','"+billPeriodRef+"','"+sourceInvoiceNo+
        "','"+sourceAccountNo+"','"+billingNumber+"','"+billingNumber+"','"+gvpnInd;
      if (costCentre.equals("NULL"))
        SQL = SQL + "',NULL,";
      else
        SQL = SQL + "','"+costCentre+"',";
      SQL = SQL +
        durationMinutes+","+durationSeconds+","+numberOfCalls+
        ","+makeLongDecimal3(billNumberCharge);
      if (discountCharges!=0)
        SQL = SQL +
          ","+makeLongDecimal5( ( (billNumberCharge*sourceDiscountTotal * 100) / discountCharges ) )+
          ","+makeLongDecimal4( billNumberCharge*10 )+//"+"+
          "+ROUND("+makeLongDecimal5( (billNumberCharge*sourceDiscountTotal * 100) / discountCharges )+",4)";
      else
        SQL = SQL + ",0,"+makeLongDecimal3(billNumberCharge);
      SQL = SQL + ",'"+lastUpdateId+"')";
      st=conn.createStatement();
      st.execute(SQL);
      st.close();
    }
    catch(java.sql.SQLException ex)
    {
      System.out.println(SQL);
      message=ex.getMessage();
      System.out.println(message);
      result = message;
    }
    return result;
  }

  public String insertAuthcodeCharge
    (String conglomCustId,
     String billedProductId,
     String billPeriodRef,
     String sourceInvoiceNo,
     String sourceAccountNo,
     String chargeTypeCode,
     String authcodeDate,
     String chargeDescription,
     long authcodeCharge,
     String lastUpdateId)
  {
    String result = "OK";
    try
    {
      if (chargeTypeCode.equals("VATH"))
        SQL =
          "INSERT INTO Source_Usage_Other_Charge "+
          "(Conglom_Cust_Id, Billed_Product_Id, Bill_Period_Ref, Source_Invoice_No, "+
          "Source_Account_No, Charge_Type_Code, Charge_Date, Charge_Description, "+
          "Charge_Amount, Last_Update_Id) "+
          "VALUES("+conglomCustId+",'"+billedProductId+"','"+billPeriodRef+"','"+sourceInvoiceNo+
          "','"+sourceAccountNo+"','"+chargeTypeCode+"','"+authcodeDate+"','"+chargeDescription+
          "',"+makeLongDecimal(authcodeCharge)+",'"+lastUpdateId+"')";
      else
        SQL =
          "INSERT INTO Source_Sundry_Charge "+
          "(Conglom_Cust_Id, Billed_Product_Id, Bill_Period_Ref, Source_Invoice_No, "+
          "Source_Account_No, Charge_Type_Code, Date_Raised, Description, "+
          "Net_Amount, Last_Update_Id) "+
          "VALUES("+conglomCustId+",'"+billedProductId+"','"+billPeriodRef+"','"+sourceInvoiceNo+
          "','"+sourceAccountNo+"','"+chargeTypeCode+"','"+authcodeDate+"','"+chargeDescription+
          "',"+makeLongDecimal(authcodeCharge)+",'"+lastUpdateId+"')";
      st=conn.createStatement();
      st.execute(SQL);
      st.close();
    }
    catch(java.sql.SQLException ex)
    {
      System.out.println(SQL);
      message=ex.getMessage();
      System.out.println(message);
      result = message;
    }
    return result;
  }

  public String insertConglomDiscount
    (String conglomCustId,
     String billedProductId,
     String billPeriodRef,
     String sourceInvoiceNo,
     String sourceAccountNo,
     String sourceAccountCheckDigit,
     long netAmount,
     long vatAmount,
     long totalAmount,
     String lastUpdateId,
     String discountItemCode1,
     long discountableAmount1,
     long discountRate1,
     long discountAmount1)
  {
    String result = "OK";
    try
    {
      SQL =
        "INSERT INTO Conglom_Discount "+
        "(Conglom_Cust_Id, Billed_Product_Id, Bill_Period_Ref, Source_Account_No, Source_Acct_Check_Digit,"+
        " Source_Invoice_No, Status, Net_Amount, Vat_Amount, Total_Amount, Last_Update_Id,"+
        " Discount_Item_Code1, Discountable_Value1,"+
        " Discount_Rate1, Discount_Amount1) "+
        "VALUES('"+conglomCustId+"','"+billedProductId+"','"+billPeriodRef+"','"+sourceAccountNo+"','"+sourceAccountCheckDigit+
        "','"+sourceInvoiceNo+"','OPEN',"+makeLongDecimal(netAmount)+","+makeLongDecimal(vatAmount)+","+makeLongDecimal(totalAmount)+
        ",'"+lastUpdateId+"','"+discountItemCode1+"',"+makeLongDecimal(discountableAmount1)+
        ","+makeLongDecimal(discountRate1)+","+makeLongDecimal(discountAmount1)+")";
      st=conn.createStatement();
      st.execute(SQL);
      st.close();
    }
    catch(java.sql.SQLException ex)
    {
      System.out.println(SQL);
      message=ex.getMessage();
      System.out.println(message);
      result = message;
    }
    return result;
  }

  public String updateConglomDiscount1
    (String conglomCustId,
     String billedProductId,
     String billPeriodRef,
     String sourceAccountNo,
     long netAmount,
     long vatAmount,
     long totalAmount,
     String lastUpdateId,
     String discountItemCode1,
     long discountableAmount1,
     long discountRate1,
     long discountAmount1)
  {
    String result = "OK";
    try
    {
      SQL =
        "UPDATE Conglom_Discount "+
        "SET Discount_Item_Code1 = '"+discountItemCode1+"', "+
        "Discountable_Amount1 = "+makeLongDecimal(discountableAmount1)+", "+
        "Discount_Rate1 = "+makeLongDecimal(discountRate1)+", "+
        "Discount_Amount1 = "+makeLongDecimal(discountAmount1)+", "+
        "WHERE Conglom_Cust_Id = "+conglomCustId+" "+
        "AND Billed_Product_Id = '"+billedProductId+"' "+
        "AND Bill_Period_Ref = '"+billPeriodRef+"', "+
        "AND Source_Account_No = '"+sourceAccountNo+"'";
      st=conn.createStatement();
      st.execute(SQL);
      st.close();
    }
    catch(java.sql.SQLException ex)
    {
      System.out.println(SQL);
      message=ex.getMessage();
      System.out.println(message);
      result = message;
    }
    return result;
  }

  public String updateConglomDiscount3
    (String conglomCustId,
     String billedProductId,
     String billPeriodRef,
     String sourceAccountNo,
     long netAmount,
     long vatAmount,
     long totalAmount,
     String lastUpdateId,
     String discountItemCode3,
     long discountableAmount3,
     long discountRate3,
     long discountAmount3)
  {
    String result = "OK";
    try
    {
      SQL =
        "UPDATE Conglom_Discount "+
        "SET Discount_Item_Code3 = '"+discountItemCode3+"', "+
        "Discountable_Amount3 = "+makeLongDecimal(discountableAmount3)+", "+
        "Discount_Rate3 = "+makeLongDecimal(discountRate3)+", "+
        "Discount_Amount3 = "+makeLongDecimal(discountAmount3)+", "+
        "WHERE Conglom_Cust_Id = "+conglomCustId+" "+
        "AND Billed_Product_Id = '"+billedProductId+"' "+
        "AND Bill_Period_Ref = '"+billPeriodRef+"', "+
        "AND Source_Account_No = '"+sourceAccountNo+"'";
      st=conn.createStatement();
      st.execute(SQL);
      st.close();
    }
    catch(java.sql.SQLException ex)
    {
      System.out.println(SQL);
      message=ex.getMessage();
      System.out.println(message);
      result = message;
    }
    return result;
  }

  public String updateConglomDiscount2
    (String conglomCustId,
     String billedProductId,
     String billPeriodRef,
     String sourceAccountNo,
     long netAmount,
     long vatAmount,
     long totalAmount,
     String lastUpdateId,
     String discountItemCode2,
     long discountableAmount2,
     long discountRate2,
     long discountAmount2)
  {
    String result = "OK";
    try
    {
      SQL =
        "UPDATE Conglom_Discount "+
        "SET Discount_Item_Code2 = '"+discountItemCode2+"', "+
        "Discountable_Amount_ = "+makeLongDecimal(discountableAmount2)+", "+
        "Discount_Rate2 = "+makeLongDecimal(discountRate2)+", "+
        "Discount_Amount2 = "+makeLongDecimal(discountAmount2)+", "+
        "WHERE Conglom_Cust_Id = "+conglomCustId+" "+
        "AND Billed_Product_Id = '"+billedProductId+"' "+
        "AND Bill_Period_Ref = '"+billPeriodRef+"', "+
        "AND Source_Account_No = '"+sourceAccountNo+"'";
      st=conn.createStatement();
      st.execute(SQL);
      st.close();
    }
    catch(java.sql.SQLException ex)
    {
      System.out.println(SQL);
      message=ex.getMessage();
      System.out.println(message);
      result = message;
    }
    return result;
  }

  public String insertVolumeDiscount
    (String conglomCustId,
     String billedProductId,
     String billPeriodRef,
     String sourceInvoiceNo,
     String sourceAccountNo,
     String chargeTypeCode,
     String billText,
     String subBillText,
     long discount,
     String lastUpdateId)
  {
    String result = "OK";
    try
    {
      SQL =
        "INSERT INTO Source_Volume_Discount "+
        "(Conglom_Cust_Id, Billed_Product_Id, Bill_Period_Ref, Source_Invoice_No, "+
        "Source_Account_No, Charge_Type_Code, Discount_Category, "+
        "Discount_Description, Discount_Amount, Last_Update_Id) "+
        "VALUES("+conglomCustId+",'"+billedProductId+"','"+billPeriodRef+"','"+sourceInvoiceNo+
        "','"+sourceAccountNo+"','"+chargeTypeCode+"','"+billText+
        "','"+subBillText+"',"+makeLongDecimal(discount)+",'"+lastUpdateId+"')";
      st=conn.createStatement();
      st.execute(SQL);
      st.close();
    }
    catch(java.sql.SQLException ex)
    {
      System.out.println(SQL);
      message=ex.getMessage();
      System.out.println(message);
      result = message;
    }
    return result;
  }

  public String insertSourceInvoice
    (String conglomCustId,
     String billedProductId,
     String billPeriodRef,
     String sourceInvoiceNo,
     String sourceAccountNo,
     String sourceAccountCheckDigit,
     String billedDate,
     String sourceConglomId,
     String billPeriodStartDate,
     String billPeriodEndDate,
     String status,
     long invoiceNetAmount,
     long invoiceVatAmount,
     long invoiceTotalAmount,
     long acctBalNetAmount,
     long acctBalVatAmount,
     long acctBalAmount,
     long oneOffCharges,
     long recurringCharges,
     long usageCharges,
     long miscCharges,
     long adjustmentTotal,
     long sourceDiscTotal,
     long conglomDiscTotal,
     long installCharges,
     long rentalCharges,
     long callinkCharges,
     long vpnCharges,
     long callCharges,
     long authcodeCharges,
     long easyUsageCharges,
     long easyQtrlyCharges,
     long sundryCharges,
     long specialCharges,
     long creditsTotal,
     long debitAdjustmtTotal,
     String lastUpdateId
    )
  {
    String result = "OK";
    try
    {
      SQL =
        "INSERT INTO Source_Invoice "+
        "(Conglom_Cust_Id,Billed_Product_Id,Bill_Period_Ref,Source_Invoice_No,Source_Account_No,"+
        " Source_Acct_Check_Digit,Billed_Date,Source_Conglom_ID,Bill_Period_Start_Date,"+
        " Bill_Period_End_Date,Status,Invoice_Net_Amount,Invoice_Vat_Amount,"+
        " Invoice_Total_Amount,Acct_Bal_Net_Amount,Acct_Bal_Vat_Amount,"+
        " Acct_Bal_Amount,Load_Method_Code,One_Off_Charges,Recurring_Charges,"+
        " Usage_Charges,Misc_Charges,Adjustment_Total,"+
        " Source_Disc_Total,Conglom_Disc_Total,Install_Charges,"+
        " Rental_Charges,Callink_Charges,VPN_Charges,"+
        " Call_Charges,AuthCode_Charges,Easy_Usage_Charges,"+
        " Easy_Qrtly_Charges,Sundry_Charges,Special_Charges,"+
        " Credits_Total,Debit_Adjustmt_Total,Last_Update_Id )"+
        "VALUES( "+
        conglomCustId+",'"+billedProductId+"','"+billPeriodRef+"','"+sourceInvoiceNo+"','"+sourceAccountNo+"','"+
        sourceAccountCheckDigit+"','"+billedDate+"','"+sourceConglomId+"','"+billPeriodStartDate+"','"+
        billPeriodEndDate+"','"+status+"',"+makeLongDecimal(invoiceNetAmount)+","+makeLongDecimal(invoiceVatAmount)+","+
        makeLongDecimal(invoiceTotalAmount)+","+makeLongDecimal(acctBalNetAmount)+","+makeLongDecimal(acctBalVatAmount)+","+
        makeLongDecimal(acctBalAmount)+",'A',"+makeLongDecimal(oneOffCharges)+","+makeLongDecimal(recurringCharges)+","+
        makeLongDecimal(usageCharges)+","+makeLongDecimal(miscCharges)+","+makeLongDecimal(adjustmentTotal)+","+
        makeLongDecimal(sourceDiscTotal)+","+makeLongDecimal(conglomDiscTotal)+","+makeLongDecimal(installCharges)+","+
        makeLongDecimal(rentalCharges)+","+makeLongDecimal(callinkCharges)+","+makeLongDecimal(vpnCharges)+","+
        makeLongDecimal(callCharges)+","+makeLongDecimal(authcodeCharges)+","+makeLongDecimal(easyUsageCharges)+","+
        makeLongDecimal(easyQtrlyCharges)+","+makeLongDecimal(sundryCharges)+","+makeLongDecimal(specialCharges)+","+
        makeLongDecimal(creditsTotal)+","+makeLongDecimal(debitAdjustmtTotal)+",'"+lastUpdateId+"')";
      st=conn.createStatement();
      st.execute(SQL);
      st.close();
    }
    catch(java.sql.SQLException ex)
    {
      System.out.println(SQL);
      message=ex.getMessage();
      System.out.println(message);
      result = message;
    }
    return result;
  }

  public String insertPCBSourceAdjustment
    (String conglomCustId,
     String billedProductId,
     String billPeriodRef,
     String sourceAccountNo,
     String sourceAccountCheckDigit,
     String creditStatus,
     String docketNumber,
     String creditNoteDate,
     long netAmount,
     long vatAmount,
     long totalAmount,
     String lastUpdateId
    )
  {
    String result = "OK";
    try
    {
      SQL =
        "INSERT INTO Source_Adjustment "+
        "(Conglom_Cust_Id,Billed_Product_Id,Bill_Period_Ref,Source_Invoice_No,Docket_Number,"+
        " Source_Account_No,Source_Acct_Check_Digit,Charge_Type_Code,Status,Date_Raised,Billed_Date,"+
        " Description,Net_Amount, Vat_Amount, "+
        " Total_Amount,Last_Update_Id )"+
        "VALUES( "+
        conglomCustId+",'"+billedProductId+"','"+billPeriodRef+"','"+docketNumber+"','"+docketNumber+"','"+
        sourceAccountNo+"','"+sourceAccountCheckDigit+"','PADJ','OPEN','"+creditNoteDate+"','"+creditNoteDate+"',"+
        "'PCB Credit Note',"+makeLongDecimal(netAmount)+","+makeLongDecimal(vatAmount)+","+
        makeLongDecimal(totalAmount)+",'"+lastUpdateId+"')";
      st=conn.createStatement();
      st.execute(SQL);
      st.close();
    }
    catch(java.sql.SQLException ex)
    {
      System.out.println(SQL);
      message=ex.getMessage();
      System.out.println(message);
      result = message;
    }
    return result;
  }

  // convert long to string to 2 decimal places
  private String makeLongDecimal( long amount)
  {
    String result = "";
    long workAmount = amount;
    if (amount<0)
      workAmount = amount * -1;
    String work = Long.toString(workAmount);
    if (workAmount>99)
      result = work.substring(0,work.length()-2)+"."+work.substring(work.length()-2,work.length());
    else if (workAmount>9)
      result = "0."+work;
    else
      result = "0.0"+work;
    if (amount<0)
      result = "-"+result;
    return result;
  }

  // convert long to string to 3 decimal places
  private String makeLongDecimal3( long amount)
  {
    String result = "";
    long workAmount = amount;
    if (amount<0)
      workAmount = amount * -1;
    String work = Long.toString(workAmount);
    if (workAmount>999)
      result = work.substring(0,work.length()-3)+"."+work.substring(work.length()-3,work.length());
    else if (workAmount>99)
      result = "0."+work;
    else if (workAmount>9)
      result = "0.0"+work;
    else
      result = "0.00"+work;
    if (amount<0)
      result = "-"+result;
    return result;
  }

  // convert long to string to 5 decimal places
  private String makeLongDecimal5( long amount)
  {
    String result = "";
    long workAmount = amount;
    if (amount<0)
      workAmount = amount * -1;
    String work = Long.toString(workAmount);
    if (workAmount>99999)
      result = work.substring(0,work.length()-5)+"."+work.substring(work.length()-5,work.length());
    else if (workAmount>9999)
      result = "0."+work;
    else if (workAmount>999)
      result = "0.0"+work;
    else if (workAmount>99)
      result = "0.00"+work;
    else if (workAmount>9)
      result = "0.000"+work;
    else
      result = "0.0000"+work;
    if (amount<0)
      result = "-"+result;
    return result;
  }

  // convert long to string to 4 decimal places
  private String makeLongDecimal4( long amount)
  {
    String result = "";
    long workAmount = amount;
    if (amount<0)
      workAmount = amount * -1;
    String work = Long.toString(workAmount);
    if (workAmount>9999)
      result = work.substring(0,work.length()-4)+"."+work.substring(work.length()-4,work.length());
    else if (workAmount>999)
      result = "0."+work;
    else if (workAmount>99)
      result = "0.0"+work;
    else if (workAmount>9)
      result = "0.00"+work;
    else
      result = "0.000"+work;
    if (amount<0)
      result = "-"+result;
    return result;
  }

  // convert long to vat amount string to 4 decimal places
  private String makeLongVATDecimal( long amount, long vatRate)
  {
    String result = "";
    long vatAmount = (amount * 100 * vatRate)/100;
    if (amount<0)
      vatAmount = vatAmount * -1;
    String work = Long.toString(vatAmount);
    if (vatAmount>9999)
      result = work.substring(0,work.length()-4)+"."+work.substring(work.length()-4,work.length());
    else if (vatAmount>999)
      result = "0."+work;
    else if (vatAmount>99)
      result = "0.0"+work;
    else if (vatAmount>9)
      result = "0.00"+work;
    else
      result = "0.000"+work;
    if (amount<0)
      result = "-"+result;
    return result;
  }

  // convert long to net plus vat amount string to 4 decimal places
  private String makeLongGrossDecimal( long amount, long vatRate)
  {
    String result = "";
    long grossAmount = (amount * (100 + vatRate))/100;
    if (amount<0)
      grossAmount = grossAmount * -1;
    String work = Long.toString(grossAmount);
    if (grossAmount>9999)
      result = work.substring(0,work.length()-4)+"."+work.substring(work.length()-4,work.length());
    else if (grossAmount>999)
      result = "0."+work;
    else if (grossAmount>99)
      result = "0.0"+work;
    else if (grossAmount>9)
      result = "0.00"+work;
    else
      result = "0.000"+work;
    if (amount<0)
      result = "-"+result;
    return result;
  }

}


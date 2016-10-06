package batch;

import java.io.*;
import java.sql.*;
import oracle.sql.*;
import oracle.jdbc.driver.OracleTypes;

public class oracleDB
{

  private Connection conn = null;
  private Statement st = null;
  private ResultSet rs = null;
  private String SQL;
  private String message;
  //private boolean monitor = true; // use if report of open cursors required
  private boolean monitor = false; // use if report of open cursors not required

  public oracleDB(Connection c)
  {
    conn = c;
  }
  
  public boolean insertCELookup(
    long reportReference,
    String reportLocation,
    long invoiceId,
    String reportName,
    String outputName)
  {
    boolean result = true;
    Statement stmt = null;
    SQL = "INSERT INTO CE_Lookup VALUES("+
            reportReference+",'"+
            reportLocation+"',"+
            invoiceId+",'"+
            reportName+"','"+
            outputName+"',"+
            "SYSDATE,'buildCELookup')";
    try    
    {
        stmt = conn.createStatement();
        stmt.execute(SQL);
        result = true;
    }
    catch(java.sql.SQLException ex)
    {         
        message=ex.getMessage();
        System.out.println(SQL);
        System.out.println(message); 
        result = false;
    }
    finally
    {
        if (monitor)
            System.out.println("getInvoiceReportDetails : "+getCurrentOpenCursors());        
        return result;
    }
  }
  
  public boolean truncateCELookup()
  {
    boolean result = true;
    Statement stmt = null;
    SQL = "TRUNCATE TABLE CE_Lookup";
    try
    {
        stmt = conn.createStatement();
        stmt.execute(SQL);
        result = true;
    }
    catch(java.sql.SQLException ex)
    {         
        message=ex.getMessage();
        System.out.println(SQL);
        System.out.println(message); 
        result = false;
    }
    finally
    {
        if (monitor)
            System.out.println("truncateCELookup : "+getCurrentOpenCursors());     
        return result;
    }    
  }
  
  public ResultSet getInvoiceReportDetails(long reportReference)
  {
    ResultSet rs = null;
    Statement stmt = null;
    SQL = "SELECT Invoice_Id, Report_Type "+
          "FROM Invoice_Report "+
          "WHERE Report_Reference = "+reportReference;
    try
    {
        stmt = conn.createStatement();
        rs = stmt.executeQuery(SQL);
    }
    catch(java.sql.SQLException ex)
    {         
        message=ex.getMessage();
        System.out.println(SQL);
        System.out.println(message); 
    }
    finally
    {
        if (monitor)
            System.out.println("getInvoiceReportDetails : "+getCurrentOpenCursors());
        return rs;
    }
  }
  
  public ResultSet getCELookupDetails(long reportReference)
  {
    ResultSet rs = null;
    Statement stmt = null;
    SQL = "SELECT Report_Location, Invoice_Id, Report_Name "+
          "FROM CE_Lookup "+
          "WHERE Report_Reference = "+reportReference;
    try
    {
        stmt = conn.createStatement();
        rs = stmt.executeQuery(SQL);
    }
    catch(java.sql.SQLException ex)
    {         
        message=ex.getMessage();
        System.out.println(SQL);
        System.out.println(message); 
    }
    finally
    {
        if (monitor)
            System.out.println("getCELookupDetails : "+getCurrentOpenCursors());
        return rs;
    }
  }

  public ResultSet getSSBSTBills(String conglomId, String billReference)
  {
    ResultSet rs = null;
    CallableStatement cstmt = null;
    try
    {
      SQL = "{ ? = call GE.Get_SSBS_TBills (?,?) }";
      cstmt = conn.prepareCall(SQL);
      cstmt.registerOutParameter(1,OracleTypes.CURSOR);
      cstmt.setString(2,conglomId);
      cstmt.setString(3,billReference);
      cstmt.execute();
      rs = (ResultSet)cstmt.getObject(1);
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      System.out.println(SQL);
      System.out.println(message);
    }
    finally
    {
      cstmt = null;
      if (monitor)
        System.out.println("getSSBSTBills : "+getCurrentOpenCursors());
      return rs;
    }
  }

  // get result set of PCB T_Bills
  public ResultSet getPCBTBills(String conglomId, String bpsd, String bped)
  {
    ResultSet rs = null;
    CallableStatement cstmt = null;
    try
    {
      SQL = "{ ? = call GE.Get_PCB_TBills (?,?,?) }";
      cstmt = conn.prepareCall(SQL);
      cstmt.registerOutParameter(1,OracleTypes.CURSOR);
      cstmt.setString(2,conglomId);
      cstmt.setString(3,bpsd);
      cstmt.setString(4,bped);
      cstmt.execute();
      rs = (ResultSet)cstmt.getObject(1);
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      System.out.println(SQL);
      System.out.println(message);
    }
    finally
    {
      cstmt = null;
      if (monitor)
        System.out.println("getPCBTBills : "+getCurrentOpenCursors());
      return rs;
    }
  }

  // get result set of prior period PCB T_Bills
  public ResultSet getPriorPCBTBills(String conglomId, String bpsd)
  {
    ResultSet rs = null;
    CallableStatement cstmt = null;
    try
    {
      SQL = "{ ? = call GE.Get_Prior_PCB_TBills (?,?) }";
      cstmt = conn.prepareCall(SQL);
      cstmt.registerOutParameter(1,OracleTypes.CURSOR);
      cstmt.setString(2,conglomId);
      cstmt.setString(3,bpsd);
      cstmt.execute();
      rs = (ResultSet)cstmt.getObject(1);
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      System.out.println(SQL);
      System.out.println(message);
    }
    finally
    {
      cstmt = null;
      if (monitor)
        System.out.println("getPriorPCBTBills : "+getCurrentOpenCursors());
      return rs;
    }
  }

  // get result set of T_Installation_Charges for SSBS
  public ResultSet getSSBSInstallCharges
    (String conglomId, String invoiceNumber, String billReference, String accountNumber)
  {
    ResultSet rs = null;
    CallableStatement cstmt = null;
    try
    {
      SQL = "{ ? = call GE.Get_SSBS_Install_Charges (?,?,?,?) }";
      cstmt = conn.prepareCall(SQL);
      cstmt.registerOutParameter(1,OracleTypes.CURSOR);
      cstmt.setString(2,conglomId);
      cstmt.setString(3,invoiceNumber);
      cstmt.setString(4,billReference);
      cstmt.setString(5,accountNumber);
      cstmt.execute();
      rs = (ResultSet)cstmt.getObject(1);
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      System.out.println(SQL);
      System.out.println(message);
    }
    finally
    {
      cstmt=null;
      if (monitor)
        System.out.println("getSSBSInstallCharges : "+getCurrentOpenCursors());
      return rs;
    }
  }

  // get result set of T_Installation_Charges for PCB
  public ResultSet getPCBInstallCharges
    (String conglomId, String invoiceNumber, String billReference, String accountNumber, String bpsd, String bped)
  {
    ResultSet rs = null;
    CallableStatement cstmt = null;
    try
    {
      SQL = "{ ? = call GE.Get_PCB_Install_Charges (?,?,?,?,?,?) }";
      cstmt = conn.prepareCall(SQL);
      cstmt.registerOutParameter(1,OracleTypes.CURSOR);
      cstmt.setString(2,conglomId);
      cstmt.setString(3,invoiceNumber);
      cstmt.setString(4,billReference);
      cstmt.setString(5,accountNumber);
      cstmt.setString(6,bpsd);
      cstmt.setString(7,bped);
      cstmt.execute();
      rs = (ResultSet)cstmt.getObject(1);
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      System.out.println(SQL);
      System.out.println(message);
    }
    finally
    {
      cstmt=null;
      if (monitor)
        System.out.println("getPCBInstallCharges : "+getCurrentOpenCursors());
      return rs;
    }
  }

  // get result set of perior period T_Installation_Charges for PCB
  public ResultSet getPriorPCBInstallCharges
    (String conglomId, String invoiceNumber, String accountNumber)
  {
    ResultSet rs = null;
    CallableStatement cstmt = null;
    try
    {
      SQL = "{ ? = call GE.Get_Prior_PCB_Install_Charges (?,?,?) }";
      cstmt = conn.prepareCall(SQL);
      cstmt.registerOutParameter(1,OracleTypes.CURSOR);
      cstmt.setString(2,conglomId);
      cstmt.setString(3,invoiceNumber);
      cstmt.setString(4,accountNumber);
      cstmt.execute();
      rs = (ResultSet)cstmt.getObject(1);
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      System.out.println(SQL);
      System.out.println(message);
    }
    finally
    {
      cstmt=null;
      if (monitor)
        System.out.println("getPriorPCBInstallCharges : "+getCurrentOpenCursors());
      return rs;
    }
  }

  // get result set of T_Account information
  public String accountCheckDigit(String shortName, String cocaId, String accountNumber)
  {
    ResultSet rs = null;
    String checkDigit = "";
    try
    {
      SQL =
        "SELECT Check_Digit "+
        "FROM T_Accounts "+
        "WHERE ACBS_Short_Name = '"+shortName+"' "+
        "AND COCA_Id = '"+cocaId+"' "+
        "AND Account_Number = '"+accountNumber+"'";
      st = conn.createStatement();
      rs = st.executeQuery(SQL);
      if (rs.next())
        checkDigit = rs.getString(1);
      st.close();
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      System.out.println(SQL);
      System.out.println(message);
    }
    finally
    {
      rs = null;
      st = null;
      if (monitor)
        System.out.println("accountCheckDigits : "+getCurrentOpenCursors());
      return checkDigit;
    }
  }

  // get result set of T_Equipment_Charges for SSBS
  public ResultSet getSSBSRentalCharges
    (String conglomId, String invoiceNumber, String billReference, String accountNumber)
  {
    ResultSet rs = null;
    CallableStatement cstmt = null;
    try
    {
      SQL = "{ ? = call GE.Get_SSBS_Rental_Charges (?,?,?,?) }";
      cstmt = conn.prepareCall(SQL);
      cstmt.registerOutParameter(1,OracleTypes.CURSOR);
      cstmt.setString(2,conglomId);
      cstmt.setString(3,invoiceNumber);
      cstmt.setString(4,billReference);
      cstmt.setString(5,accountNumber);
      cstmt.execute();
      rs = (ResultSet)cstmt.getObject(1);
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      System.out.println(SQL);
      System.out.println(message);
    }
    finally
    {
      cstmt=null;
      if (monitor)
        System.out.println("getSSBSRentalCharges : "+getCurrentOpenCursors());
      return rs;
    }
  }

  // get result set of T_Callink_Charges for SSBS
  public ResultSet getSSBSCallinkCharges
    (String conglomId, String invoiceNumber, String billReference, String accountNumber)
  {
    ResultSet rs = null;
    CallableStatement cstmt = null;
    try
    {
      SQL = "{ ? = call GE.Get_SSBS_Callink_Charges (?,?,?,?) }";
      cstmt = conn.prepareCall(SQL);
      cstmt.registerOutParameter(1,OracleTypes.CURSOR);
      cstmt.setString(2,conglomId);
      cstmt.setString(3,invoiceNumber);
      cstmt.setString(4,billReference);
      cstmt.setString(5,accountNumber);
      cstmt.execute();
      rs = (ResultSet)cstmt.getObject(1);
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      System.out.println(SQL);
      System.out.println(message);
    }
    finally
    {
      cstmt=null;
      if (monitor)
        System.out.println("getSSBSCallinkCharges : "+getCurrentOpenCursors());
      return rs;
    }
  }

  // get result set of T_Sundry_Charges for SSBS
  public ResultSet getSSBSSundryCharges
    (String conglomId, String invoiceNumber, String billReference, String accountNumber)
  {
    ResultSet rs = null;
    CallableStatement cstmt = null;
    try
    {
      SQL = "{ ? = call GE.Get_SSBS_Sundry_Charges (?,?,?,?) }";
      cstmt = conn.prepareCall(SQL);
      cstmt.registerOutParameter(1,OracleTypes.CURSOR);
      cstmt.setString(2,conglomId);
      cstmt.setString(3,invoiceNumber);
      cstmt.setString(4,billReference);
      cstmt.setString(5,accountNumber);
      cstmt.execute();
      rs = (ResultSet)cstmt.getObject(1);
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      System.out.println(SQL);
      System.out.println(message);
    }
    finally
    {
      cstmt=null;
      if (monitor)
        System.out.println("getSSBSSundryCharges : "+getCurrentOpenCursors());
      return rs;
    }
  }

  // get result set of T_VPN_Charges for SSBS
  public ResultSet getSSBSVPNCharges
    (String conglomId, String invoiceNumber, String billReference, String accountNumber)
  {
    ResultSet rs = null;
    CallableStatement cstmt = null;
    try
    {
      SQL = "{ ? = call GE.Get_SSBS_VPN_Charges (?,?,?,?) }";
      cstmt = conn.prepareCall(SQL);
      cstmt.registerOutParameter(1,OracleTypes.CURSOR);
      cstmt.setString(2,conglomId);
      cstmt.setString(3,invoiceNumber);
      cstmt.setString(4,billReference);
      cstmt.setString(5,accountNumber);
      cstmt.execute();
      rs = (ResultSet)cstmt.getObject(1);
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      System.out.println(SQL);
      System.out.println(message);
    }
    finally
    {
      cstmt=null;
      if (monitor)
        System.out.println("getSSBSVPNCharges : "+getCurrentOpenCursors());
      return rs;
    }
  }

  // get result set of T_Call_Charges for SSBS
  public ResultSet getSSBSCallCharges
    (String conglomId, String invoiceNumber, String billReference, String accountNumber)
  {
    ResultSet rs = null;
    CallableStatement cstmt = null;
    try
    {
      SQL = "{ ? = call GE.Get_SSBS_Call_Charges (?,?,?,?) }";
      cstmt = conn.prepareCall(SQL);
      cstmt.registerOutParameter(1,OracleTypes.CURSOR);
      cstmt.setString(2,conglomId);
      cstmt.setString(3,invoiceNumber);
      cstmt.setString(4,billReference);
      cstmt.setString(5,accountNumber);
      cstmt.execute();
      rs = (ResultSet)cstmt.getObject(1);
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      System.out.println(SQL);
      System.out.println(message);
    }
    finally
    {
      cstmt=null;
      if (monitor)
        System.out.println("getSSBSCallCharges : "+getCurrentOpenCursors());
      return rs;
    }
  }

  // get result set of T_Volume Discounts for SSBS
  public ResultSet getSSBSVolumeDiscounts
    (String conglomId, String invoiceNumber, String billReference, String accountNumber)
  {
    ResultSet rs = null;
    CallableStatement cstmt = null;
    try
    {
      SQL = "{ ? = call GE.Get_SSBS_Volume_Discounts (?,?,?,?) }";
      cstmt = conn.prepareCall(SQL);
      cstmt.registerOutParameter(1,OracleTypes.CURSOR);
      cstmt.setString(2,conglomId);
      cstmt.setString(3,invoiceNumber);
      cstmt.setString(4,billReference);
      cstmt.setString(5,accountNumber);
      cstmt.execute();
      rs = (ResultSet)cstmt.getObject(1);
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      System.out.println(SQL);
      System.out.println(message);
    }
    finally
    {
      cstmt=null;
      if (monitor)
        System.out.println("getSSBSVolumeDiscounts : "+getCurrentOpenCursors());
      return rs;
    }
  }

  // get result set of T_Authcode_Charges for SSBS
  public ResultSet getSSBSAuthcodeCharges
    (String conglomId, String invoiceNumber, String billReference, String accountNumber)
  {
    ResultSet rs = null;
    CallableStatement cstmt = null;
    try
    {
      SQL = "{ ? = call GE.Get_SSBS_Authcode_Charges (?,?,?,?) }";
      cstmt = conn.prepareCall(SQL);
      cstmt.registerOutParameter(1,OracleTypes.CURSOR);
      cstmt.setString(2,conglomId);
      cstmt.setString(3,invoiceNumber);
      cstmt.setString(4,billReference);
      cstmt.setString(5,accountNumber);
      cstmt.execute();
      rs = (ResultSet)cstmt.getObject(1);
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      System.out.println(SQL);
      System.out.println(message);
    }
    finally
    {
      cstmt=null;
      if (monitor)
        System.out.println("getSSBSAuthcodeCharges : "+getCurrentOpenCursors());
      return rs;
    }
  }

  // get result set of T_Easy_Access_Quarterly_Charge for SSBS
  public ResultSet getSSBSEasyAccessQtrlyCharges
    (String conglomId, String invoiceNumber, String billReference, String accountNumber)
  {
    ResultSet rs = null;
    CallableStatement cstmt = null;
    try
    {
      SQL = "{ ? = call GE.Get_SSBS_Easy_Qtrly_Charges (?,?,?,?) }";
      cstmt = conn.prepareCall(SQL);
      cstmt.registerOutParameter(1,OracleTypes.CURSOR);
      cstmt.setString(2,conglomId);
      cstmt.setString(3,invoiceNumber);
      cstmt.setString(4,billReference);
      cstmt.setString(5,accountNumber);
      cstmt.execute();
      rs = (ResultSet)cstmt.getObject(1);
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      System.out.println(SQL);
      System.out.println(message);
    }
    finally
    {
      cstmt=null;
      if (monitor)
        System.out.println("getSSBSEasyAccessQtrlyCharges : "+getCurrentOpenCursors());
      return rs;
    }
  }

  // get result set of T_Easy_Access_Usage_Charges for SSBS
  public ResultSet getSSBSEasyAccessUsageCharges
    (String conglomId, String invoiceNumber, String billReference, String accountNumber)
  {
    ResultSet rs = null;
    CallableStatement cstmt = null;
    try
    {
      SQL = "{ ? = call GE.Get_SSBS_Easy_Usage_Charges (?,?,?,?) }";
      cstmt = conn.prepareCall(SQL);
      cstmt.registerOutParameter(1,OracleTypes.CURSOR);
      cstmt.setString(2,conglomId);
      cstmt.setString(3,invoiceNumber);
      cstmt.setString(4,billReference);
      cstmt.setString(5,accountNumber);
      cstmt.execute();
      rs = (ResultSet)cstmt.getObject(1);
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      System.out.println(SQL);
      System.out.println(message);
    }
    finally
    {
      cstmt=null;
      if (monitor)
        System.out.println("getSSBSEasyAccessUasgeCharges : "+getCurrentOpenCursors());
      return rs;
    }
  }

  // get result set of T_Billing_Number_Charges for SSBS
  public ResultSet getSSBSBillingNumberCharges
    (String conglomId, String invoiceNumber, String billReference)
  {
    ResultSet rs = null;
    CallableStatement cstmt = null;
    try
    {
      SQL = "{ ? = call GE.Get_SSBS_Billing_No_Charges (?,?,?) }";
      cstmt = conn.prepareCall(SQL);
      cstmt.registerOutParameter(1,OracleTypes.CURSOR);
      cstmt.setString(2,conglomId);
      cstmt.setString(3,invoiceNumber);
      cstmt.setString(4,billReference);
      cstmt.execute();
      rs = (ResultSet)cstmt.getObject(1);
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      System.out.println(SQL);
      System.out.println(message);
    }
    finally
    {
      cstmt=null;
      if (monitor)
        System.out.println("getSSBSBillingNumberCharges : "+getCurrentOpenCursors());
      return rs;
    }
  }

  // get result set of consol reference items
    ResultSet getConsolRefItems(String conglomId, String sourceSystemId, String ldRefType)
  {
    ResultSet rs = null;
    CallableStatement cstmt = null;
    try
    {
      SQL = "{ ? = call GE.Get_Consol_Ref_Items (?,?,?) }";
      cstmt = conn.prepareCall(SQL);
      cstmt.registerOutParameter(1,OracleTypes.CURSOR);
      cstmt.setString(2,conglomId);
      cstmt.setString(3,sourceSystemId);
      cstmt.setString(4,ldRefType);
      cstmt.execute();
      rs = (ResultSet)cstmt.getObject(1);
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      System.out.println(SQL);
      System.out.println(message);
    }
    finally
    {
      cstmt=null;
      if (monitor)
        System.out.println("getConsolRefItems : "+getCurrentOpenCursors());
      return rs;
    }
  }

  // get result set of billsum reference items
    ResultSet getBillsumRefItems(String conglomId, String ldRefType)
  {
    ResultSet rs = null;
    CallableStatement cstmt = null;
    try
    {
      SQL = "{ ? = call GE.Get_Billsum_Ref_Items (?,?) }";
      cstmt = conn.prepareCall(SQL);
      cstmt.registerOutParameter(1,OracleTypes.CURSOR);
      cstmt.setString(2,conglomId);
      cstmt.setString(3,ldRefType);
      cstmt.execute();
      rs = (ResultSet)cstmt.getObject(1);
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      System.out.println(SQL);
      System.out.println(message);
    }
    finally
    {
      cstmt=null;
      if (monitor)
        System.out.println("getBillSumRefItems : "+getCurrentOpenCursors());
      return rs;
    }
  }

  // get result set of T_Credit_Notes for PCB
  public ResultSet getPCBCreditNotes
    (String conglomId, String BPSD, String BPED)
  {
    ResultSet rs = null;
    CallableStatement cstmt = null;
    try
    {
      SQL = "{ ? = call GE.Get_PCB_Credits (?,?,?) }";
      cstmt = conn.prepareCall(SQL);
      cstmt.registerOutParameter(1,OracleTypes.CURSOR);
      cstmt.setString(2,conglomId);
      cstmt.setString(3,BPSD);
      cstmt.setString(4,BPED);
      cstmt.execute();
      rs = (ResultSet)cstmt.getObject(1);
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      System.out.println(SQL);
      System.out.println(message);
    }
    finally
    {
      cstmt=null;
      if (monitor)
        System.out.println("getPCBCreditNotes : "+getCurrentOpenCursors());
      return rs;
    }
  }

  // get result set of T_Credits for SSBS
  public ResultSet getSSBSCredits
    (String conglomId, String invoiceNumber, String billReference, String accountNumber, String vatExemptInd, long vatRate)
  {
    ResultSet rs = null;
    CallableStatement cstmt = null;
    try
    {
      SQL = "{ ? = call GE.Get_SSBS_Credits (?,?,?,?,?,?) }";
      cstmt = conn.prepareCall(SQL);
      cstmt.registerOutParameter(1,OracleTypes.CURSOR);
      cstmt.setString(2,conglomId);
      cstmt.setString(3,invoiceNumber);
      cstmt.setString(4,billReference);
      cstmt.setString(5,accountNumber);
      cstmt.setString(6,vatExemptInd);
      cstmt.setLong(7,vatRate);
      cstmt.execute();
      System.out.println(SQL);
      rs = (ResultSet)cstmt.getObject(1);
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      System.out.println(message);
    }
    finally
    {
      cstmt=null;
      if (monitor)
        System.out.println("getSSBDCredits : "+getCurrentOpenCursors());
      return rs;
    }
  }

  // get result set of T_Equipment_Charges and T_Previous_Charges for PCB
  public ResultSet getPCBRentalCharges
    (String conglomId, String invoiceNumber, String billReference, String accountNumber, String bpsd, String bped)
  {
    ResultSet rs = null;
    CallableStatement cstmt = null;
    try
    {
      SQL = "{ ? = call GE.Get_PCB_Rental_Charges (?,?,?,?,?,?) }";
      cstmt = conn.prepareCall(SQL);
      cstmt.registerOutParameter(1,OracleTypes.CURSOR);
      cstmt.setString(2,conglomId);
      cstmt.setString(3,invoiceNumber);
      cstmt.setString(4,billReference);
      cstmt.setString(5,accountNumber);
      cstmt.setString(6,bpsd);
      cstmt.setString(7,bped);
      cstmt.execute();
      rs = (ResultSet)cstmt.getObject(1);
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      System.out.println(SQL);
      System.out.println(message);
    }
    finally
    {
      cstmt=null;
      if (monitor)
        System.out.println("getPCBRentalCharges : "+getCurrentOpenCursors());
      return rs;
    }
  }

  // get result set of prior period T_Equipment_Charges and T_Previous_Charges for PCB
  public ResultSet getPriorPCBRentalCharges
    (String conglomId, String invoiceNumber, String accountNumber)
  {
    ResultSet rs = null;
    CallableStatement cstmt = null;
    try
    {
      SQL = "{ ? = call GE.Get_Prior_PCB_Rental_Charges (?,?,?) }";
      cstmt = conn.prepareCall(SQL);
      cstmt.registerOutParameter(1,OracleTypes.CURSOR);
      cstmt.setString(2,conglomId);
      cstmt.setString(3,invoiceNumber);
      cstmt.setString(4,accountNumber);
      cstmt.execute();
      rs = (ResultSet)cstmt.getObject(1);
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      System.out.println(SQL);
      System.out.println(message);
    }
    finally
    {
      cstmt=null;
      if (monitor)
        System.out.println("getPriorPCBRentalCharges : "+getCurrentOpenCursors());
      return rs;
    }
  }

  // get result set of T_Sundry_Charges for PCB
  public ResultSet getPCBSundryCharges
    (String conglomId, String invoiceNumber, String billReference, String accountNumber, String bpsd, String bped)
  {
    ResultSet rs = null;
    CallableStatement cstmt = null;
    try
    {
      SQL = "{ ? = call GE.Get_PCB_Sundry_Charges (?,?,?,?,?,?) }";
      cstmt = conn.prepareCall(SQL);
      cstmt.registerOutParameter(1,OracleTypes.CURSOR);
      cstmt.setString(2,conglomId);
      cstmt.setString(3,invoiceNumber);
      cstmt.setString(4,billReference);
      cstmt.setString(5,accountNumber);
      cstmt.setString(6,bpsd);
      cstmt.setString(7,bped);
      cstmt.execute();
      rs = (ResultSet)cstmt.getObject(1);
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      System.out.println(SQL);
      System.out.println(message);
    }
    finally
    {
      cstmt=null;
      if (monitor)
        System.out.println("getPCBSundryCharges : "+getCurrentOpenCursors());
      return rs;
    }
  }

  // get result set of prior period T_Sundry_Charges for PCB
  public ResultSet getPriorPCBSundryCharges
    (String conglomId, String invoiceNumber, String accountNumber)
  {
    ResultSet rs = null;
    CallableStatement cstmt = null;
    try
    {
      SQL = "{ ? = call GE.Get_Prior_PCB_Sundry_Charges (?,?,?) }";
      cstmt = conn.prepareCall(SQL);
      cstmt.registerOutParameter(1,OracleTypes.CURSOR);
      cstmt.setString(2,conglomId);
      cstmt.setString(3,invoiceNumber);
      cstmt.setString(4,accountNumber);
      cstmt.execute();
      rs = (ResultSet)cstmt.getObject(1);
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      System.out.println(SQL);
      System.out.println(message);
    }
    finally
    {
      cstmt=null;
      if (monitor)
        System.out.println("getPriorPCBSundryCharges : "+getCurrentOpenCursors());
      return rs;
    }
  }

  // not now used - show current cursor usage
  public int getCurrentOpenCursors()
  {
    ResultSet rs = null;
    int cursors = -1;
    try
    {
      String SQL = "select count(*) AS COUNT from v$open_cursor where user_name like 'CONGOMGR%'";
      st = conn.createStatement();
      rs = st.executeQuery(SQL);
      if (rs.next())
        cursors = rs.getInt("COUNT");
      st.close();
      rs.close();
    }
    catch (SQLException e)
    {
      System.out.println("SQLException in getCurrentOpenCursors(Connection conn): "+e);
    }
    finally
    {
      rs = null;
      return cursors;
    }
  }
  
  // insert feed control row (ebilling)
  public long insertFeedControl(
          String billingSource,
          String customerReference,
          String invoiceNo,
          String accountNumber,
          String accountName,
          String ISOCurrencyCode,
          String taxPointDate,
          String periodFromDate,
          String periodToDate,
          String invoiceTotal,
          String controlFilename)
  {
    CallableStatement cstmt = null;
    long id = 0;
    try
    {
      SQL = "{ ? = call New_Feed.Insert_Feed_Control (?,?,?,?,?,?,?,?,?,?,?) }";
      cstmt = conn.prepareCall(SQL);
      cstmt.registerOutParameter(1,OracleTypes.INTEGER);
      cstmt.setString(2,billingSource);
      cstmt.setString(3,customerReference);
      cstmt.setString(4,invoiceNo);
      cstmt.setString(5,accountNumber);
      cstmt.setString(6,accountName);
      cstmt.setString(7,ISOCurrencyCode);
      cstmt.setString(8,taxPointDate);
      cstmt.setString(9,periodFromDate);
      cstmt.setString(10,periodToDate);
      cstmt.setString(11,invoiceTotal);
      cstmt.setString(12,controlFilename);
      cstmt.execute();
      id = cstmt.getLong(1);
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      System.out.println(SQL);
      System.out.println(message);
    }
    finally
    {
      cstmt = null;
      if (monitor)
        System.out.println("insertControlFeed : "+getCurrentOpenCursors());
      return id;
    }
  }
  
  // insert report feed row (ebilling)
  public long insertFeedReport(long feedControlId, String reportName, String displayName)
  {
    CallableStatement cstmt = null;
    long id = 0;
    try
    {
      SQL = "{ ? = call New_Feed.Insert_Feed_Report (?,?,?) }";
      cstmt = conn.prepareCall(SQL);
      cstmt.registerOutParameter(1,OracleTypes.INTEGER);
      cstmt.setLong(2,feedControlId);
      cstmt.setString(3,reportName);
      cstmt.setString(4,displayName);
      cstmt.execute();
      id = cstmt.getLong(1);
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      System.out.println(SQL);
      System.out.println(message);
    }
    finally
    {
      cstmt = null;
      if (monitor)
        System.out.println("insertReportFeed : "+getCurrentOpenCursors());
      return id;
    }
  }
  
  // create or update invoice (ebilling)
  public long createInvoice(
      long accountId, 
      String invoiceNo, 
      String ISOCurrencyCode, 
      String taxPointDate, 
      String periodFromDate, 
      String periodToDate, 
      String invoiceTotal)
  {
    CallableStatement cstmt = null;
    long result = 0;
    try
    {
      SQL = "{ ? = call New_Feed.Create_Invoice(?,?,?,?,?,?,?) }";
      cstmt = conn.prepareCall(SQL);
      cstmt.registerOutParameter(1,OracleTypes.INTEGER);
      cstmt.setLong(2,accountId);
      cstmt.setString(3,invoiceNo);
      cstmt.setString(4,ISOCurrencyCode);
      cstmt.setString(5,taxPointDate);
      cstmt.setString(6,periodFromDate);
      cstmt.setString(7,periodToDate);
      cstmt.setString(8,invoiceTotal);
      cstmt.execute();
      result = cstmt.getLong(1);
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      System.out.println(SQL);
      System.out.println(message);
    }
    finally
    {
      cstmt = null;
      if (monitor)
        System.out.println("createInvoice : "+getCurrentOpenCursors());
      return result;
    }
  }
  
  // create or update attachment (ebilling)
  public long createAttachment(
      String location, 
      String name, 
      long id, 
      String type, 
      String attachmentDate, 
      String itemType)
  {
    CallableStatement cstmt = null;
    long result = 0;
    try
    {
      SQL = "{ ? = call New_Feed.Create_Attachment(?,?,?,?,?,?) }";
      cstmt = conn.prepareCall(SQL);
      cstmt.registerOutParameter(1,OracleTypes.INTEGER);
      cstmt.setString(2,location);
      cstmt.setString(3,name);
      cstmt.setLong(4,id);
      cstmt.setString(5,type);
      cstmt.setString(6,attachmentDate);
      cstmt.setString(7,itemType);
      cstmt.execute();
      result = cstmt.getLong(1);
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      System.out.println(SQL);
      System.out.println(message);
    }
    finally
    {
      cstmt = null;
      if (monitor)
        System.out.println("createAttachment : "+getCurrentOpenCursors());
      return result;
    }
  }
  // get storm account id
  public long getStormAccountId(String accountNumber)
  {
    long result = -777;
    CallableStatement cstmt = null;
    try
    {
        SQL = "{ ? = call Storm.Get_Account_Id(?) }";
        cstmt = conn.prepareCall(SQL);
        cstmt.registerOutParameter(1,OracleTypes.INTEGER);
        cstmt.setString(2,accountNumber);          
        cstmt.execute();
        result = cstmt.getLong(1);
    }
    catch(java.sql.SQLException ex)
    {
        message=ex.getMessage();
        System.out.println(SQL);
        System.out.println(message);
    }
    finally
    {
        cstmt = null;
        if (monitor)
        System.out.println("getAccountId : "+getCurrentOpenCursors());
        return result;
    }
  }      
  
  // Create or update an attachment
  public long createAttachment(
      String location,
      String name,
      long id,
      String type,
      String dateText,
      String dateFormat,
      String itemType)
  {     
    long result = -777;
    CallableStatement cstmt = null;
    try
    {
        SQL = "{ ? = call Storm.Create_Attachment(?,?,?,?,?,?,?) }";
        cstmt = conn.prepareCall(SQL);
        cstmt.registerOutParameter(1,OracleTypes.INTEGER);
        cstmt.setString(2,location);
        cstmt.setString(3,name);
        cstmt.setLong(4,id);
        cstmt.setString(5,type);
        cstmt.setString(6,dateText);
        cstmt.setString(7,dateFormat);
        cstmt.setString(8,itemType);
        cstmt.execute();
        result = cstmt.getLong(1);
    }
    catch(java.sql.SQLException ex)
    {
        message=ex.getMessage();
        System.out.println(SQL);
        System.out.println(message);
    }
    finally
    {
        cstmt = null;
        if (monitor)
        System.out.println("createAttachment : "+getCurrentOpenCursors());
        return result;
    } 
  }    
  
  // Create or update an invoice
  public long createStormInvoice(
      Long accountId,
      String invoiceNo,
      String dateText)
  {     
    long result = -777;
    CallableStatement cstmt = null;
    try
    {
        SQL = "{ ? = call Storm.Create_Invoice(?,?,?) }";
        cstmt = conn.prepareCall(SQL);
        cstmt.registerOutParameter(1,OracleTypes.INTEGER);
        cstmt.setLong(2,accountId);
        cstmt.setString(3,invoiceNo);
        cstmt.setString(4,dateText);
        cstmt.execute();
        result = cstmt.getLong(1);
    }
    catch(java.sql.SQLException ex)
    {
        message=ex.getMessage();
        System.out.println(SQL);
        System.out.println(message);
    }
    finally
    {
        cstmt = null;
        if (monitor)
        System.out.println("createStormInvoice : "+getCurrentOpenCursors());
        return result;
    } 
  }    
  
  // Find an invoice for a monthly CDR
  public long findStormInvoice(
      Long accountId,
      String invoiceNo,
      String dateText)
  {     
    long result = -777;
    CallableStatement cstmt = null;
    try
    {
        SQL = "{ ? = call Storm.Find_Invoice(?,?,?) }";
        cstmt = conn.prepareCall(SQL);
        cstmt.registerOutParameter(1,OracleTypes.INTEGER);
        cstmt.setLong(2,accountId);
        cstmt.setString(3,invoiceNo);
        cstmt.setString(4,dateText);
        cstmt.execute();
        result = cstmt.getLong(1);
    }
    catch(java.sql.SQLException ex)
    {
        message=ex.getMessage();
        System.out.println(SQL);
        System.out.println(message);
    }
    finally
    {
        cstmt = null;
        if (monitor)
        System.out.println("findStormInvoice : "+getCurrentOpenCursors());
        return result;
    } 
  }   
  
  // Validate a supplied date range
  public String validateDateRange( String startDate, String endDate)
  {
    String result = "";
    CallableStatement cstmt = null;  
    try
    {
        SQL = "{ ? = call Check_Date_Range(?,?) }";
        cstmt = conn.prepareCall(SQL);
        cstmt.registerOutParameter(1,OracleTypes.VARCHAR);
        cstmt.setString(2,startDate);
        cstmt.setString(3,endDate);
        cstmt.execute();
        result = cstmt.getString(1);
    }
    catch(java.sql.SQLException ex)
    {
        message=ex.getMessage();
        System.out.println(SQL);
        System.out.println(message);
    }
    finally
    {
        cstmt = null;
        if (monitor)
        System.out.println("validateDateRange : "+getCurrentOpenCursors());
        return result;
    }       
  }
  
  // Find an invoice for a monthly CDR
  public String getInvoiceNo(Long invoiceId)
  {     
    String result = "NOVALUE";
    CallableStatement cstmt = null;
    try
    {
        SQL = "{ ? = call Storm.Find_Invoice_No(?) }";
        cstmt = conn.prepareCall(SQL);
        cstmt.registerOutParameter(1,OracleTypes.VARCHAR);
        cstmt.setLong(2,invoiceId);
        cstmt.execute();
        result = cstmt.getString(1);
    }
    catch(java.sql.SQLException ex)
    {
        message=ex.getMessage();
        System.out.println(SQL);
        System.out.println(message);
    }
    finally
    {
        cstmt = null;
        if (monitor)
        System.out.println("getInvoiceNo : "+getCurrentOpenCursors());
        return result;
    } 
  }
    
  // create or update account (ebilling)
  public long getAccountId(
      long customerId, 
      String customerReference, 
      String billingSource, 
      String accountNumber, 
      String accountName)
  {
    CallableStatement cstmt = null;
    long result = 0;
    try
    {
      SQL = "{ ? = call New_Feed.Get_Account_Id(?,?,?,?,?) }";
      cstmt = conn.prepareCall(SQL);
      cstmt.registerOutParameter(1,OracleTypes.INTEGER);
      cstmt.setLong(2,customerId);
      cstmt.setString(3,customerReference);
      cstmt.setString(4,billingSource);
      cstmt.setString(5,accountNumber);
      cstmt.setString(6,accountName);
      cstmt.execute();
      result = cstmt.getLong(1);
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      System.out.println(SQL);
      System.out.println(message);
    }
    finally
    {
      cstmt = null;
      if (monitor)
        System.out.println("getAccountId : "+getCurrentOpenCursors());
      return result;
    }
  }
  
  // update feed control status (ebilling)
  public long updateFeedControlStatus(long feedControlId, String status, String processingMessage)
  {
    CallableStatement cstmt = null;
    long result = 0;
    try
    {
      SQL = "{ ? = call New_Feed.Update_Feed_Control_Status (?,?,?) }";
      cstmt = conn.prepareCall(SQL);
      cstmt.registerOutParameter(1,OracleTypes.INTEGER);
      cstmt.setLong(2,feedControlId);
      cstmt.setString(3,status);
      cstmt.setString(4,processingMessage);
      cstmt.execute();
      result = cstmt.getLong(1);
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      System.out.println(SQL);
      System.out.println(message);
    }
    finally
    {
      cstmt = null;
      if (monitor)
        System.out.println("updateFeedControlStatus : "+getCurrentOpenCursors());
      return result;
    }
  }
  
  // update feed report status (ebilling)
  public long updateFeedReportStatus(long feedReportId, String status, String processingMessage)
  {
    CallableStatement cstmt = null;
    long result = 0;
    try
    {
      SQL = "{ ? = call New_Feed.Update_Feed_Report_Status (?,?,?) }";
      cstmt = conn.prepareCall(SQL);
      cstmt.registerOutParameter(1,OracleTypes.INTEGER);
      cstmt.setLong(2,feedReportId);
      cstmt.setString(3,status);
      cstmt.setString(4,processingMessage);
      cstmt.execute();
      result = cstmt.getLong(1);
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      System.out.println(SQL);
      System.out.println(message);
    }
    finally
    {
      cstmt = null;
      if (monitor)
        System.out.println("updateFeedReportStatus : "+getCurrentOpenCursors());
      return result;
    }
  }
    
  // get result set of Feed Control with status of 'Loaded' or 'In Suspense' (ebilling)
  public ResultSet getOutstandingFeedControl()
  {
    ResultSet rs = null;
    CallableStatement cstmt = null;
    try
    {
      SQL = "{ ? = call New_Feed.Get_Outstanding_Feed_Control () }";
      cstmt = conn.prepareCall(SQL);
      cstmt.registerOutParameter(1,OracleTypes.CURSOR);
      cstmt.execute();
      rs = (ResultSet)cstmt.getObject(1);
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      System.out.println(SQL);
      System.out.println(message);
    }
    finally
    {
      cstmt=null;
      if (monitor)
        System.out.println("getOutstandingFeedControl : "+getCurrentOpenCursors());
      return rs;
    }
  }
  // get result set of Feed Report with status of 'Loaded' or 'In Suspense' (ebilling)
  public ResultSet getOutstandingFeedReport()
  {
    ResultSet rs = null;
    CallableStatement cstmt = null;
    try
    {
      SQL = "{ ? = call New_Feed.Get_Outstanding_Feed_Report () }";
      cstmt = conn.prepareCall(SQL);
      cstmt.registerOutParameter(1,OracleTypes.CURSOR);
      cstmt.execute();
      rs = (ResultSet)cstmt.getObject(1);
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      System.out.println(SQL);
      System.out.println(message);
    }
    finally
    {
      cstmt=null;
      if (monitor)
        System.out.println("getOutstandingFeedReport : "+getCurrentOpenCursors());
      return rs;
    }
  }
  
  // get result set of invoice reports 
  public ResultSet getInvoiceReports(String billingSource, String startDate, String endDate)
  {
    ResultSet rs = null;
    CallableStatement cstmt = null;
    try
    {
      SQL = "{ ? = call Attachment_Extract.Get_Invoice_Reports (?,?,?) }";
      cstmt = conn.prepareCall(SQL);
      cstmt.registerOutParameter(1,OracleTypes.CURSOR);
      cstmt.setString(2,billingSource);
      cstmt.setString(3,startDate);
      cstmt.setString(4,endDate);
      cstmt.execute();
      rs = (ResultSet)cstmt.getObject(1);
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      System.out.println(SQL);
      System.out.println(message);
    }
    finally
    {
      cstmt=null;
      if (monitor)
        System.out.println("getInvoiceReports : "+getCurrentOpenCursors());
      return rs;
    }
  }
  
  // get result set of monthly CDRs
  public ResultSet getMonthlyCDRs(String billingSource, String startDate, String endDate)
  {
    ResultSet rs = null;
    CallableStatement cstmt = null;
    try
    {
      SQL = "{ ? = call Attachment_Extract.Get_Monthly_CDRs (?,?,?) }";
      cstmt = conn.prepareCall(SQL);
      cstmt.registerOutParameter(1,OracleTypes.CURSOR);
      cstmt.setString(2,billingSource);
      cstmt.setString(3,startDate);
      cstmt.setString(4,endDate);
      cstmt.execute();
      rs = (ResultSet)cstmt.getObject(1);
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      System.out.println(SQL);
      System.out.println(message);
    }
    finally
    {
      cstmt=null;
      if (monitor)
        System.out.println("getMonthlyCDRs : "+getCurrentOpenCursors());
      return rs;
    }
  }
    
  // get result set of daily CDRs
  public ResultSet getDailyCDRs(String billingSource, String startDate, String endDate)
  {
    ResultSet rs = null;
    CallableStatement cstmt = null;
    try
    {
      SQL = "{ ? = call Attachment_Extract.Get_Daily_CDRs (?,?,?) }";
      cstmt = conn.prepareCall(SQL);
      cstmt.registerOutParameter(1,OracleTypes.CURSOR);
      cstmt.setString(2,billingSource);
      cstmt.setString(3,startDate);
      cstmt.setString(4,endDate);
      cstmt.execute();
      rs = (ResultSet)cstmt.getObject(1);
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      System.out.println(SQL);
      System.out.println(message);
    }
    finally
    {
      cstmt=null;
      if (monitor)
        System.out.println("getDailyCDRs : "+getCurrentOpenCursors());
      return rs;
    }
  }  
    
  // get result set of attachments
  public ResultSet getAttachments(String billingSource, String startDate, String endDate)
  {
    ResultSet rs = null;
    CallableStatement cstmt = null;
    try
    {
      SQL = "{ ? = call Attachment_Extract.Get_Monthly_Attachments (?,?,?) }";
      cstmt = conn.prepareCall(SQL);
      cstmt.registerOutParameter(1,OracleTypes.CURSOR);
      cstmt.setString(2,billingSource);
      cstmt.setString(3,startDate);
      cstmt.setString(4,endDate);
      cstmt.execute();
      rs = (ResultSet)cstmt.getObject(1);
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      System.out.println(SQL);
      System.out.println(message);
    }
    finally
    {
      cstmt=null;
      if (monitor)
        System.out.println("getAttachments : "+getCurrentOpenCursors());
      return rs;
    }
  }     
    
  // get result set of attachments
  public ResultSet getInvoiceDetails(long invoiceId)
  {
    ResultSet rs = null;
    CallableStatement cstmt = null;
    try
    {
      SQL = "{ ? = call Get_Invoice_Details (?) }";
      cstmt = conn.prepareCall(SQL);
      cstmt.registerOutParameter(1,OracleTypes.CURSOR);
      cstmt.setLong(2,invoiceId);
      cstmt.execute();
      rs = (ResultSet)cstmt.getObject(1);
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      System.out.println(SQL);
      System.out.println(message);
    }
    finally
    {
      cstmt=null;
      if (monitor)
        System.out.println("getInvoiceDetails : "+getCurrentOpenCursors());
      return rs;
    }
  } 
  
}


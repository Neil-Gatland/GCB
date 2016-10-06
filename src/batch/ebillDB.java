package batch;

import java.io.*;
import java.sql.*;

public class ebillDB
{

  private Connection conn = null;
  private Statement st = null;
  private ResultSet rs = null;
  private String SQL;
  private String message;

  public ebillDB(Connection c)
  {
    conn = c;
  }

  // check if job is running for a specific billing source
  public boolean running(String billingSource)
  {
    boolean result = true;
    String jobId = "";
    SQL = "SELECT ISNULL(Job_Id,'NULL') FROM GIVN_REF..Repository_Control "+
          "WHERE Billing_Source = '"+billingSource+"'";
    //System.out.println(SQL);
    try
    {
      st = conn.createStatement();
      rs = st.executeQuery(SQL);
      if (rs.next())
      {
        jobId = rs.getString(1);
        //System.out.println(jobId);
      }
      st.close();
    }
    catch(java.sql.SQLException e)
    {
      System.out.println(
        "Error getting job id from repository control for "+
        billingSource+" : "+
        e.toString());
    }
    if (jobId.startsWith("NULL"))
      result = false;
    return result;
  }

  // mark job as running for a specific billing source
  public void setJobId(String billingSource)
  {
    boolean result = true;
    SQL = "UPDATE GIVN_REF..Repository_Control "+
          "SET Job_Id = 'running' "+
          "WHERE Billing_Source = '"+billingSource+"'";
    //System.out.println(SQL);
    try
    {
      st = conn.createStatement();
      st.execute(SQL);
      st.close();
    }
    catch(java.sql.SQLException e)
    {
      System.out.println(
        "Error setting job id on repository control for "+
        billingSource+" : "+
        e.toString());
    }
  }

  // set run control for normally completed job
  public void setFileNo(String billingSource, int newFileNo)
  {
    boolean result = true;
    SQL = "UPDATE GIVN_REF..Repository_Control "+
          "SET Last_File_No = "+newFileNo+ ", Last_Creation_Date = getdate() "+
          "WHERE Billing_Source = '"+billingSource+"'";
    //System.out.println(SQL);
    try
    {
      st = conn.createStatement();
      st.execute(SQL);
      st.close();
    }
    catch(java.sql.SQLException e)
    {
      System.out.println(
        "Error setting job id on repository control for "+
        billingSource+" : "+
        e.toString());
    }
  }

  // mark job as not running for a specific billing source
  public void resetJobId(String billingSource)
  {
    boolean result = true;
    SQL = "UPDATE GIVN_REF..Repository_Control "+
          "SET Job_Id = NULL "+
          "WHERE Billing_Source = '"+billingSource+"'";
    //System.out.println(SQL);
    try
    {
      st = conn.createStatement();
      st.execute(SQL);
      st.close();
    }
    catch(java.sql.SQLException e)
    {
      System.out.println(
        "Error resetting job id on repository control for "+
        billingSource+" : "+
        e.toString());
    }
  }

  // get invoices for data not yet extracted
  public ResultSet getDataInvoiceNos()
  {
    ResultSet rs = null;
    CallableStatement cstmt = null;
    int result = 0;
    try
    {
      SQL = "{ ? = call ebill_data_invoices ()}";
      cstmt = conn.prepareCall(SQL);
      cstmt.registerOutParameter(1, java.sql.Types.INTEGER);
      rs = cstmt.executeQuery();
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      System.out.println(message);
    }
    finally
    {
      cstmt = null;
      return rs;
    }
  }

  // get last file number used from repository control
  public long getLastFileNo(String billingSource)
  {
    long lastFileNo = 0;
    SQL = "SELECT Last_File_No FROM GIVN_REF..Repository_Control "+
          "WHERE Billing_Source = '"+billingSource+"'";
    //System.out.println(SQL);
    try
    {
      st = conn.createStatement();
      rs = st.executeQuery(SQL);
      if (rs.next())
      {
        lastFileNo = rs.getLong(1);
        //System.out.println(lastFileNo);
      }
      st.close();
    }
    catch(java.sql.SQLException e)
    {
      System.out.println(
        "Error getting last file no from repository control for "+
        billingSource+" : "+
        e.toString());
    }
    return lastFileNo;
  }

  // set new last file no for a specific billing source
  public void resetFileNo(long newLastFileNo, String billingSource)
  {
    boolean result = true;
    SQL = "UPDATE GIVN_REF..Repository_Control "+
          "SET Last_File_No = "+newLastFileNo+", "+
          "Last_Creation_Date = GETDATE() "+
          "WHERE Billing_Source = '"+billingSource+"'";
    //System.out.println(SQL);
    try
    {
      st = conn.createStatement();
      st.execute(SQL);
      st.close();
    }
    catch(java.sql.SQLException e)
    {
      System.out.println(
        "Error updating file no on repository control for "+
        billingSource+" : "+
        e.toString());
    }
  }

  // get account details for a specific data invoice
  public ResultSet getDataAccountDets(String globalCustomerId, String invoiceNo)
  {
    ResultSet rs = null;
    CallableStatement cstmt = null;
    int result = 0;
    try
    {
      SQL = "{ ? = call ebill_data_account_details (?,?)}";
      cstmt = conn.prepareCall(SQL);
      cstmt.registerOutParameter(1, java.sql.Types.INTEGER);
      cstmt.setString(2,globalCustomerId);
      cstmt.setString(3,invoiceNo);
      rs = cstmt.executeQuery();
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      System.out.println(message);
    }
    finally
    {
      cstmt = null;
      return rs;
    }
  }

  // get GCDBR values for a specific data invoice
  public ResultSet getDataGCDBRs(String globalCustomerId, String invoiceNo)
  {
    ResultSet rs = null;
    CallableStatement cstmt = null;
    int result = 0;
    try
    {
      SQL = "{ ? = call ebill_data_gcdbrs (?,?)}";
      cstmt = conn.prepareCall(SQL);
      cstmt.registerOutParameter(1, java.sql.Types.INTEGER);
      cstmt.setString(2,globalCustomerId);
      cstmt.setString(3,invoiceNo);
      rs = cstmt.executeQuery();
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      System.out.println(message);
    }
    finally
    {
      cstmt = null;
      return rs;
    }
  }

  // get charge line data for a specific GCDBR
  public ResultSet getDataGCDBRLine
    (String globalCustomerId,
     String invoiceRegion,
     String invoiceNo,
     String globalCustomerDivisionId,
     String billingRegion)
  {
    ResultSet rs = null;
    CallableStatement cstmt = null;
    int result = 0;
    try
    {
      SQL = "{ ? = call ebill_data_gcdbr_line (?,?,?,?,?)}";
      cstmt = conn.prepareCall(SQL);
      cstmt.registerOutParameter(1, java.sql.Types.INTEGER);
      cstmt.setString(2,globalCustomerId);
      cstmt.setString(3,invoiceRegion);
      cstmt.setString(4,invoiceNo);
      cstmt.setString(5,globalCustomerDivisionId);
      cstmt.setString(6,billingRegion);
      rs = cstmt.executeQuery();
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      System.out.println(message);
    }
    finally
    {
      cstmt = null;
      return rs;
    }
  }

  // get charge detail data for a specific GCDBR
  public ResultSet getDataGCDBRDetail
    (String globalCustomerId,
     String invoiceRegion,
     String invoiceNo,
     String globalCustomerDivisionId,
     String billingRegion)
  {
    ResultSet rs = null;
    CallableStatement cstmt = null;
    int result = 0;
    try
    {
      SQL = "{ ? = call ebill_data_gcdbr_det (?,?,?,?,?)}";
      cstmt = conn.prepareCall(SQL);
      cstmt.registerOutParameter(1, java.sql.Types.INTEGER);
      cstmt.setString(2,globalCustomerId);
      cstmt.setString(3,invoiceRegion);
      cstmt.setString(4,invoiceNo);
      cstmt.setString(5,globalCustomerDivisionId);
      cstmt.setString(6,billingRegion);
      rs = cstmt.executeQuery();
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      System.out.println(message);
    }
    finally
    {
      cstmt = null;
      return rs;
    }
  }

  // get invoice tax line data
  public ResultSet getDataTaxLine
    (String globalCustomerId,
     String invoiceRegion,
     String invoiceNo)
  {
    ResultSet rs = null;
    CallableStatement cstmt = null;
    int result = 0;
    try
    {
      SQL = "{ ? = call ebill_data_tax_line (?,?,?)}";
      cstmt = conn.prepareCall(SQL);
      cstmt.registerOutParameter(1, java.sql.Types.INTEGER);
      cstmt.setString(2,globalCustomerId);
      cstmt.setString(3,invoiceRegion);
      cstmt.setString(4,invoiceNo);
      rs = cstmt.executeQuery();
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      System.out.println(message);
    }
    finally
    {
      cstmt = null;
      return rs;
    }
  }

  // get invoice tax payable detail data
  public ResultSet getDataTPDet
    (String globalCustomerId,
     String invoiceNo)
  {
    ResultSet rs = null;
    CallableStatement cstmt = null;
    int result = 0;
    try
    {
      SQL = "{ ? = call ebill_data_tp_det (?,?)}";
      cstmt = conn.prepareCall(SQL);
      cstmt.registerOutParameter(1, java.sql.Types.INTEGER);
      cstmt.setString(2,globalCustomerId);
      cstmt.setString(3,invoiceNo);
      rs = cstmt.executeQuery();
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      System.out.println(message);
    }
    finally
    {
      cstmt = null;
      return rs;
    }
  }

  // get invoice vertex detail data
  public ResultSet getDataVertexDet
    (String globalCustomerId,
     String invoiceNo)
  {
    ResultSet rs = null;
    CallableStatement cstmt = null;
    int result = 0;
    try
    {
      SQL = "{ ? = call ebill_data_vertex_det (?,?)}";
      cstmt = conn.prepareCall(SQL);
      cstmt.registerOutParameter(1, java.sql.Types.INTEGER);
      cstmt.setString(2,globalCustomerId);
      cstmt.setString(3,invoiceNo);
      rs = cstmt.executeQuery();
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      System.out.println(message);
    }
    finally
    {
      cstmt = null;
      return rs;
    }
  }

  // get invoice adjustment tax detail data
  public ResultSet getDataAdjTaxDet
    (String globalCustomerId,
     String invoiceNo)
  {
    ResultSet rs = null;
    CallableStatement cstmt = null;
    int result = 0;
    try
    {
      SQL = "{ ? = call ebill_data_adjtax_det (?,?)}";
      cstmt = conn.prepareCall(SQL);
      cstmt.registerOutParameter(1, java.sql.Types.INTEGER);
      cstmt.setString(2,globalCustomerId);
      cstmt.setString(3,invoiceNo);
      rs = cstmt.executeQuery();
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      System.out.println(message);
    }
    finally
    {
      cstmt = null;
      return rs;
    }
  }

  // get adjustment detail data
  public ResultSet getDataAdjDet
    (String globalCustomerId,
     String invoiceNo)
  {
    ResultSet rs = null;
    CallableStatement cstmt = null;
    int result = 0;
    try
    {
      SQL = "{ ? = call ebill_data_adj_det (?,?)}";
      cstmt = conn.prepareCall(SQL);
      cstmt.registerOutParameter(1, java.sql.Types.INTEGER);
      cstmt.setString(2,globalCustomerId);
      cstmt.setString(3,invoiceNo);
      rs = cstmt.executeQuery();
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      System.out.println(message);
    }
    finally
    {
      cstmt = null;
      return rs;
    }
  }

  // get discount detail data
  public ResultSet getDataDiscDet
    (String globalCustomerId,
     String invoiceNo)
  {
    ResultSet rs = null;
    CallableStatement cstmt = null;
    int result = 0;
    try
    {
      SQL = "{ ? = call ebill_data_discounts_det (?,?)}";
      cstmt = conn.prepareCall(SQL);
      cstmt.registerOutParameter(1, java.sql.Types.INTEGER);
      cstmt.setString(2,globalCustomerId);
      cstmt.setString(3,invoiceNo);
      rs = cstmt.executeQuery();
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      System.out.println(message);
    }
    finally
    {
      cstmt = null;
      return rs;
    }
  }

  // get invoice total for a specific data invoice
  public ResultSet getDataInvoiceTotal(String globalCustomerId, String invoiceNo)
  {
    ResultSet rs = null;
    CallableStatement cstmt = null;
    int result = 0;
    try
    {
      SQL = "{ ? = call ebill_data_invoice_total (?,?)}";
      cstmt = conn.prepareCall(SQL);
      cstmt.registerOutParameter(1, java.sql.Types.INTEGER);
      cstmt.setString(2,globalCustomerId);
      cstmt.setString(3,invoiceNo);
      rs = cstmt.executeQuery();
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      System.out.println(message);
    }
    finally
    {
      cstmt = null;
      return rs;
    }
  }

  // set data invoice as extracted
  public boolean setDataInvoiceExtracted
    (String globalCustomerId,
     String invoiceNo,
     long fileNo)
  {
    boolean result = false;
    CallableStatement cstmt = null;
    try
    {
      SQL = "{ ? = call ebill_data_set_ext (?,?,?)}";
      cstmt = conn.prepareCall(SQL);
      cstmt.registerOutParameter(1, java.sql.Types.INTEGER);
      cstmt.setString(2,globalCustomerId);
      cstmt.setString(3,invoiceNo);
      cstmt.setLong(4,fileNo);
      cstmt.execute();
      rs = cstmt.executeQuery();
      result = true;
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      System.out.println(message);
    }
    finally
    {
      cstmt = null;
      rs = null;
      return result;
    }
  }

  // backout all data invoices above last good file number
  public boolean backoutDataInvoicesExtracted
    (long lastFileNo)
  {
    boolean result = false;
    CallableStatement cstmt = null;
    try
    {
      SQL = "{ ? = call ebill_data_backout_all (?)}";
      cstmt = conn.prepareCall(SQL);
      cstmt.registerOutParameter(1, java.sql.Types.INTEGER);
      cstmt.setLong(2,lastFileNo);
      rs = cstmt.executeQuery();
      result = true;
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      System.out.println(message);
    }
    finally
    {
      cstmt = null;
      rs = null;
      return result;
    }
  }

  // get invoices for conglom not yet extracted
  public ResultSet getConglomInvoiceNos()
  {
    ResultSet rs = null;
    CallableStatement cstmt = null;
    int result = 0;
    try
    {
      SQL = "{ ? = call ebill_conglom_invoice_nos ()}";
      cstmt = conn.prepareCall(SQL);
      cstmt.registerOutParameter(1, java.sql.Types.INTEGER);
      rs = cstmt.executeQuery();
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      System.out.println(message);
    }
    finally
    {
      cstmt = null;
      return rs;
    }
  }
  // get invoices for conglom not yet extracted
  public ResultSet getGdialInvoiceNos()
  {
    ResultSet rs = null;
    CallableStatement cstmt = null;
    int result = 0;
    try
    {
      SQL = "{ ? = call ebill_gdial_invoice_nos ()}";
      cstmt = conn.prepareCall(SQL);
      cstmt.registerOutParameter(1, java.sql.Types.INTEGER);
      rs = cstmt.executeQuery();
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      System.out.println(message);
    }
    finally
    {
      cstmt = null;
      return rs;
    }
  }

  // set conglom invoice as extracted
  public boolean setConglomInvoiceExtracted
    (String invoiceNo,
     long fileNo)
  {
    boolean result = false;
    CallableStatement cstmt = null;
    try
    {
      SQL = "{ ? = call ebill_conglom_set_ext (?,?)}";
      cstmt = conn.prepareCall(SQL);
      cstmt.registerOutParameter(1, java.sql.Types.INTEGER);
      cstmt.setString(2,invoiceNo);
      cstmt.setLong(3,fileNo);
      cstmt.execute();
      result = true;
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      System.out.println(message);
    }
    finally
    {
      cstmt = null;
      return result;
    }
  }
  // set globaldial invoice as extracted
  public boolean setGdialInvoiceExtracted
    (String invoiceNo,
     long fileNo)
  {
    boolean result = false;
    CallableStatement cstmt = null;
    try
    {
      SQL = "{ ? = call ebill_gdial_set_ext (?,?)}";
      cstmt = conn.prepareCall(SQL);
      cstmt.registerOutParameter(1, java.sql.Types.INTEGER);
      cstmt.setString(2,invoiceNo);
      cstmt.setLong(3,fileNo);
      cstmt.execute();
      result = true;
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      System.out.println(message);
    }
    finally
    {
      cstmt = null;
      return result;
    }
  }

  // backout all conglom invoices above last good file number
  public boolean backoutConglomInvoicesExtracted
    (long lastFileNo)
  {
    boolean result = false;
    CallableStatement cstmt = null;
    try
    {
      SQL = "{ ? = call ebill_conglom_backout_all (?)}";
      cstmt = conn.prepareCall(SQL);
      cstmt.registerOutParameter(1, java.sql.Types.INTEGER);
      cstmt.setLong(2,lastFileNo);
      rs = cstmt.executeQuery();
      result = true;
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      System.out.println(message);
    }
    finally
    {
      cstmt = null;
      rs = null;
      return result;
    }
  }
  // backout all globaldial invoices above last good file number
  public boolean backoutGdialInvoicesExtracted
    (long lastFileNo)
  {
    boolean result = false;
    CallableStatement cstmt = null;
    try
    {
      SQL = "{ ? = call ebill_gdial_backout_all (?)}";
      cstmt = conn.prepareCall(SQL);
      cstmt.registerOutParameter(1, java.sql.Types.INTEGER);
      cstmt.setLong(2,lastFileNo);
      rs = cstmt.executeQuery();
      result = true;
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      System.out.println(message);
    }
    finally
    {
      cstmt = null;
      rs = null;
      return result;
    }
  }

  // get account details for a specific conglom invoice
  public ResultSet getConglomAccountDets(String invoiceNo)
  {
    ResultSet rs = null;
    CallableStatement cstmt = null;
    int result = 0;
    try
    {
      SQL = "{ ? = call ebill_conglom_account_dets (?)}";
      cstmt = conn.prepareCall(SQL);
      cstmt.registerOutParameter(1, java.sql.Types.INTEGER);
      cstmt.setString(2,invoiceNo);
      rs = cstmt.executeQuery();
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      System.out.println(message);
    }
    finally
    {
      cstmt = null;
      return rs;
    }
  }

  // get globaldial invoice details
  public ResultSet getGdialUsageDets(String globalCustomerId, String invoiceNo)
  {
    ResultSet rs = null;
    CallableStatement cstmt = null;
    int result = 0;
    try
    {
      SQL = "{ ? = call globaldial_invoice_detail (?,?)}";
      cstmt = conn.prepareCall(SQL);
      cstmt.registerOutParameter(1, java.sql.Types.INTEGER);
      cstmt.setString(2,globalCustomerId);
      cstmt.setString(3,invoiceNo);
      rs = cstmt.executeQuery();
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      System.out.println(message);
    }
    finally
    {
      cstmt = null;
      return rs;
    }
  }

  // get globaldial account details
  public ResultSet getGdialAccountDets(String globalCustomerId, String accountName)
  {
    ResultSet rs = null;
    CallableStatement cstmt = null;
    int result = 0;
    try
    {
      SQL = "{ ? = call givn_ref..ebill_account_dets (?,?)}";
      cstmt = conn.prepareCall(SQL);
      cstmt.registerOutParameter(1, java.sql.Types.INTEGER);
      cstmt.setString(2,globalCustomerId);
      cstmt.setString(3,accountName);
      rs = cstmt.executeQuery();
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      System.out.println(message);
    }
    finally
    {
      cstmt = null;
      return rs;
    }
  }

  // get globaldail invoice details
  public ResultSet getGdialInvoiceDets(String globalCustomerId, String invoiceNo)
  {
    ResultSet rs = null;
    CallableStatement cstmt = null;
    int result = 0;
    try
    {
      SQL = "{ ? = call globaldial_invoice (?,?)}";
      cstmt = conn.prepareCall(SQL);
      cstmt.registerOutParameter(1, java.sql.Types.INTEGER);
      cstmt.setString(2,globalCustomerId);
      cstmt.setString(3,invoiceNo);
      rs = cstmt.executeQuery();
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      System.out.println(message);
    }
    finally
    {
      cstmt = null;
      return rs;
    }
  }
  // get additional globaldial account details
  public ResultSet getGdialData(String globalCustomerId, String invoiceNo)
  {
    ResultSet rs = null;
    CallableStatement cstmt = null;
    int result = 0;
    try
    {
      SQL = "{ ? = call ebill_gdial_data (?,?)}";
      cstmt = conn.prepareCall(SQL);
      cstmt.registerOutParameter(1, java.sql.Types.INTEGER);
      cstmt.setString(2,globalCustomerId);
      cstmt.setString(3,invoiceNo);
      rs = cstmt.executeQuery();
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      System.out.println(message);
    }
    finally
    {
      cstmt = null;
      return rs;
    }
  }

  // get invoice details for a specific conglom invoice
  public ResultSet getConglomInvoiceDetails(String invoiceNo)
  {
    ResultSet rs = null;
    CallableStatement cstmt = null;
    int result = 0;
    try
    {
      SQL = "{ ? = call cb_invoice (?)}";
      cstmt = conn.prepareCall(SQL);
      cstmt.registerOutParameter(1, java.sql.Types.INTEGER);
      cstmt.setString(2,invoiceNo);
      rs = cstmt.executeQuery();
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      System.out.println(message);
    }
    finally
    {
      cstmt = null;
      return rs;
    }
  }

  // get product details for a specific conglom invoice
  public ResultSet getConglomInvoiceProductDetails(String invoiceNo)
  {
    ResultSet rs = null;
    CallableStatement cstmt = null;
    int result = 0;
    try
    {
      SQL = "{ ? = call cb_invoice_details (?)}";
      cstmt = conn.prepareCall(SQL);
      cstmt.registerOutParameter(1, java.sql.Types.INTEGER);
      cstmt.setString(2,invoiceNo);
      rs = cstmt.executeQuery();
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      System.out.println(message);
    }
    finally
    {
      cstmt = null;
      return rs;
    }
  }

  // get bill period dates for a specific conglom invoice
  public ResultSet getConglomInvoiceBillPeriods(String invoiceNo)
  {
    ResultSet rs = null;
    CallableStatement cstmt = null;
    int result = 0;
    try
    {
      SQL = "{ ? = call ebill_conglom_bill_periods (?)}";
      cstmt = conn.prepareCall(SQL);
      cstmt.registerOutParameter(1, java.sql.Types.INTEGER);
      cstmt.setString(2,invoiceNo);
      rs = cstmt.executeQuery();
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      System.out.println(message);
    }
    finally
    {
      cstmt = null;
      return rs;
    }
  }

  // get switched bill numbers no local data details for a specific conglom invoice
  public ResultSet getReportDetails(String invoiceNo, String reportName)
  {
    ResultSet rs = null;
    CallableStatement cstmt = null;
    int result = 0;
    try
    {
      SQL = "{ ? = call "+reportName+" (?)}";
      cstmt = conn.prepareCall(SQL);
      cstmt.registerOutParameter(1, java.sql.Types.INTEGER);
      cstmt.setString(2,invoiceNo);
      rs = cstmt.executeQuery();
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      if (!reportName.startsWith("cb_ssvo_discount_summary_subt"))
        System.out.println(message);
    }
    finally
    {
      cstmt = null;
      return rs;
    }
  }

  // get switched bill numbers no local data details for a specific conglom invoice
  public long getLDCount(String invoiceNo, String reportType, String billedProductId)
  {
    ResultSet rs = null;
    CallableStatement cstmt = null;
    long result = 0;
    try
    {
      SQL = "{ ? = call ebill_conglom_ld_check_count (?,?,?)}";
      cstmt = conn.prepareCall(SQL);
      cstmt.registerOutParameter(1, java.sql.Types.INTEGER);
      cstmt.setString(2,invoiceNo);
      cstmt.setString(3,reportType);
      cstmt.setString(4,billedProductId);
      rs = cstmt.executeQuery();
      if (rs.next())
        result = rs.getLong("Check_Count");
    }
    catch(java.sql.SQLException ex)
    {
      message=ex.getMessage();
      System.out.println(message);
    }
    finally
    {
      cstmt = null;
      rs = null;
      return result;
    }
  }

}


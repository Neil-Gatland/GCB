package batch;

// Required imports
import java.io.*;
import java.sql.*;
import java.util.Iterator;
import org.apache.poi.ss.usermodel.*;

public class passThroughPreProcessor 
{    
  // Global class variables
   
  // SQLServer environment variables
  private String sqlServer, sqlServerDb, sqlServerUser, sqlServerPassword;
  private Connection sqlConn;
  private sqlServerDB ssDB;
  
  // Processing variables
  private String passThroughType, genesisCustomer,servicePartner, country, profile, productId; 
  
  // Directory variables
  private String logDir, preProcessDir, cdrDir, processedDir, rejectDir;
  
  // Various string variables
  private String message, fullPassThroughType;
  
  // Spreadsheet processing variables
  private FileInputStream fis;
  private Workbook workbook;
  private Sheet sheet;
  private Row row;
  private Cell cell;
  
  // File variables
  private BufferedWriter logBW, cdrBW;
  
  // Global class constants
  //
  // String constants
  private static String USCORE = "_";
  private static String COMMA = ",";
  private static String CDREXT = ".cdr";
  private static String HDR = "H";
  private static String DET = "R";
  private static String TLR = "T";
  private static String INTPREFIX = "00";
  
  // initialise supplied parameters
  private passThroughPreProcessor()
  {
    passThroughType="";
    genesisCustomer="";
    servicePartner=""; 
    country=""; 
    profile="";
    productId="";
    sqlServer="";
    sqlServerDb="";
    sqlServerUser="";
    sqlServerPassword="";
    logDir="";
    preProcessDir="";
    processedDir="";
    cdrDir="";
    rejectDir="";
  }    

  // main class links straight to control processing
  public static void main(String[] args)
  {
    passThroughPreProcessor ptpp = new passThroughPreProcessor();
    ptpp.control();
  }
  
  // controls processing of Pass-Through files
  private void control()
  {
      // Get properties from properties file
      if (propertiesSet())
      {    
          // Open log file
          if (openLogFile())
          {
              // Decode Pass-Through file type
              if (passThroughType.startsWith("HUTSOFT"))
                 fullPassThroughType = "Hutchinson Soft Bill"; 
              else
                  fullPassThroughType = "UNKNOWN";
              // Stop if unrecognied Pass-Through file type
              if (fullPassThroughType.startsWith("UNKNOWN"))
              {
                  message = "Pass-Through Type of "+passThroughType+" not recognised";
                  writeToLogFile(message);
              }
              else
              {                  
                  // Process all files in the Pre-Process directory
                  File ppDir = new File(preProcessDir);
                  File[] preProcessArray = ppDir.listFiles();
                  int preProcessFileCount=0;
                  for (int i = 0; i < preProcessArray.length; i++)
                  {                     
                     File ppFile = preProcessArray[i];
                     preProcessFileCount++;
                     // Specific processing for Hutchison Soft Bill
                     if (passThroughType.startsWith("HUTSOFT"))
                         processHutSoft(ppFile);
                  }
                  if (preProcessFileCount==0)
                  {
                     message = "No Pass-Through files to process";
                     writeToLogFile(message);
                  }
              }
          }
          // Close log file
          closeLogFile();
      }
  }
  
  private void processHutSoft(File softFile)
  {
    String ppFileName = softFile.getName();
    message = "Processing file "+ppFileName;
    writeToLogFile(message); 
    int nextSeqNo = getFileSeqNo();
    if (nextSeqNo>0)
    {
       String formattedSeqNo = formatNextSeqNo(nextSeqNo);
       String cdrFileName = getCDRFileName(formattedSeqNo);
       if (openFileInputStream(softFile))
       {
           if (createWorkbook(fis))
           {
               // Check that the spreadsheet only has a single workbook
               // and sheetname is as expected from maximum CDR call month
               int sheetCount = workbook.getNumberOfSheets();
               String sheetName = workbook.getSheetName(0);
               String expectedSheetName = getExpectedSheetName();
               if (sheetCount>1)
               {
                   // move failed file to reject directory
                   message = "   More than one workbook in the spreadsheet - moving file to reject directory";
                   writeToLogFile(message);
                   File rejSoftFile = new File(rejectDir+File.separator+ppFileName);
                   if (!softFile.renameTo(rejSoftFile))
                   {
                       message = "   IO Error failed to move soft file "+ppFileName+" to reject directory";
                       writeToLogFile(message);
                   }
               }
               else if ((!expectedSheetName.equals(sheetName))&&(!expectedSheetName.equals("NODATA")))
               {
                   // move failed file to reject directory
                   message = 
                       "   Expected worksheet name of "+expectedSheetName+
                       " - Actual worksheet name is "+sheetName+
                       " - moving file to reject directory";
                   writeToLogFile(message);
                   File rejSoftFile = new File(rejectDir+File.separator+ppFileName);
                   if (!softFile.renameTo(rejSoftFile))
                   {
                       message = "   IO Error failed to move soft file "+ppFileName+" to reject directory";
                       writeToLogFile(message);
                   }                   
               }
               else
               {
                   // Can now open cdr file and create header record
                   if (openCDRFile(cdrFileName))
                   {
                       // Process workbook and write out cdr records
                       int detailCount = 0;
                       sheet = workbook.getSheetAt(0);
                       boolean headerReached = false;
                       String phoneNo="", calledPhoneNo="", date="", time="";  
                       double duration = 0, ratedAmount=0;
                       for (Iterator rIt = sheet.rowIterator(); rIt.hasNext(); )
                       {
                           row = (Row)rIt.next();
                           cell = row.getCell(0);
                           // only check cells that are not null
                           if (cell!=null)
                           {
                               String cellContents = cell.getStringCellValue();
                               if (!cellContents.endsWith("Total"))
                               {
                                   if (headerReached)
                                   {
                                       phoneNo = row.getCell(2).getStringCellValue();
                                       calledPhoneNo = row.getCell(7).getStringCellValue();
                                       date = row.getCell(3).getStringCellValue();
                                       time = row.getCell(4).getStringCellValue();
                                       duration = row.getCell(10).getNumericCellValue();
                                       ratedAmount = row.getCell(11).getNumericCellValue();
                                       //System.out.println(phoneNo+"|"+calledPhoneNo+"|"+date+"|"+time+"|"+duration+"|"+ratedAmount);
                                       String cdrLine = 
                                           formatHutSoftLine(
                                               phoneNo,
                                               calledPhoneNo,
                                               date,
                                               time,
                                               duration,
                                               ratedAmount);
                                       writeToCDRFile(cdrLine);
                                       detailCount++;
                                   }
                                   if (cellContents.equals("User Name"))
                                       headerReached = true;
                               }                                    
                           }  
                       }
                       // Close cdr file
                       closeCDRFile(cdrFileName,detailCount);
                       writeToLogFile("   CDR file "+cdrFileName+" created in CDR directory");
                       // Move soft bill file to processed directory
                       File newSoftFile = new File(processedDir+File.separator+ppFileName);
                       if (!softFile.renameTo(newSoftFile))
                       {
                           message = "   IO Error failed to move soft file "+ppFileName+" to processed directory";
                           writeToLogFile(message);
                       }
                       // update Genesis File Sequence with new seq no
                       if (!updateFileSeqNo(nextSeqNo))
                       {
                           message = "   Failed to update Genesis File Sequence with latest seq no";
                           writeToLogFile(message);
                       }
                   }
               }               
           }
           closeFileInputStream();
       }
    }
  }
  
  // format a CDR line for HutSoft input
  private String formatHutSoftLine(
    String phoneNo,
    String calledPhoneNo,
    String date,
    String time,
    double duration,
    double ratedAmount)
  {
      String csvLine = 
        DET+COMMA+
        phoneNo+COMMA+
        INTPREFIX+calledPhoneNo+COMMA+
        "XX"+COMMA+
        date.substring(6, 10)+date.substring(3, 5)+date.substring(0,2)+COMMA+
        time.replace(":", "")+"00"+COMMA+
        formatDurationHutSoft(duration)+COMMA+
        "R"+COMMA+
        "HKD"+COMMA+
        ratedAmount+COMMA+
        formatRate(duration,ratedAmount)+COMMA+
        "P";
      return csvLine;
  }
  
  private String formatRate(double duration, double ratedAmount)
  {
      double rate = ratedAmount / duration;
      String rateString = Double.toString(rate);
      // determine position of full stop
      int periodPos = -1;
      for( int i=0; i<rateString.length(); i++ )
      {
          String testChar = rateString.substring(i, i+1);
          if (testChar.equals("."))
              periodPos = i;
      }
      // seperate units and decimal parts of number 
      String leftPart = rateString.substring(0, periodPos);
      String rightPart = rateString.substring(periodPos+1, rateString.length());
      // left pad units with zeroes to length of 3
      switch (leftPart.length())
      {
          case 1: leftPart = "00" + leftPart;
          break;
          case 2: leftPart = "0" + leftPart;
          break;
          case 3: // do nothing;
          break;
          default: leftPart = leftPart.substring(leftPart.length() - 3, leftPart.length()); // should not happen!
          break;
      }
      // right pad decimals with zeroes to length of 5
      
      switch (rightPart.length())
      {
          case 1: rightPart = rightPart + "0000";
          break;
          case 2: rightPart = rightPart + "000";
          break;
          case 3: rightPart = rightPart + "00";
          break;
          case 4: rightPart = rightPart + "0";
          break;
          case 5: // do nothing;
          break;
          default: rightPart = rightPart.substring(0, 5);
          break;
      }
      String finalRate = leftPart + rightPart;
      return finalRate;
  }
  
  private String formatDurationHutSoft(double duration)
  {
      String formattedDuration = "";
      // convert from minutes to seconds
      long seconds = (long)duration * 60;
      // add in any required leading zeroes
      formattedDuration = Long.toString(seconds);
      switch (formattedDuration.length())
      {
          case 1: formattedDuration = "000000" + formattedDuration;
          break;
          case 2: formattedDuration = "00000" + formattedDuration;
          break;
          case 3: formattedDuration = "0000" + formattedDuration;
          break;
          case 4: formattedDuration = "000" + formattedDuration;
          break;
          case 5: formattedDuration = "00" + formattedDuration;
          break;
          case 6: formattedDuration = "0" + formattedDuration;
          break;
      }
      formattedDuration = formattedDuration+"0"; // add in tenth of second       
      return formattedDuration;
  }
  
  private String getExpectedSheetName()
  {
      String sheetName = "XXXXXX";
      if (openSQLServer())
      {      
          sheetName = ssDB.getExpectedSheetName(genesisCustomer, country, servicePartner);
          // close connection
          sqlConn = null;
      }
      //System.out.println(sheetName);
      return sheetName;
  }
  
  // opens file input steam for soft file
  private boolean openFileInputStream(File softFile)
  {
      boolean result = true;
      try
      {
          fis = new FileInputStream(softFile);
      }
      catch (IOException e)
      {
        writeToLogFile("   IO Error opening file input stream : "+e.toString());  
        result = false;
      }
      return result;
  }
  // opens file input steam for soft file
  private void closeFileInputStream()
  {
      
      try
      {
          fis.close();
      }
      catch (IOException e)
      {
        writeToLogFile("   IO Error closing file input stream : "+e.toString());
      }
  }
  
  // opens file input steam for soft file
  private boolean createWorkbook(FileInputStream fis)
  {
      boolean result = true;
      try
      {
          workbook = WorkbookFactory.create(fis);
      }
      catch (IOException e)
      {
        writeToLogFile("   IO Error creating workbook : "+e.toString());  
        result = false;
      }catch (org.apache.poi.openxml4j.exceptions.InvalidFormatException e)
       {
          writeToLogFile("   Format Exception Error creating workbook : "+e.toString());          
       }
      return result;
  }
  
  private String getCDRFileName (String fileSeqNo)
  {
      String fileDate = new java.util.Date().toString();
      String timestamp = 
                fileDate.substring(24,28)+decodeMonth(fileDate.substring(4,7))+
                fileDate.substring(8,10)+fileDate.substring(11,13)+
                fileDate.substring(14,16)+fileDate.substring(17,19);
      String result = 
                genesisCustomer+USCORE+
                servicePartner+USCORE+
                country+USCORE+
                profile+USCORE+
                productId+USCORE+
                fileSeqNo+USCORE+
                timestamp+
                CDREXT;;
      return result;
  }
  
  // Read Genesis File Sequence to determine next seq no to use for output file
  private int getFileSeqNo()
  {
      int seqNo = 0; 
      if (openSQLServer())
      {          
          seqNo = ssDB.getNextSeqNo(genesisCustomer, servicePartner, country , profile, productId);
          // reset back to 1 if max value of 9999 reached
          if (seqNo==10000)
              seqNo=1;
          // close connection
          sqlConn = null;
      }
      // set to minus one if not found
      else
          seqNo = -1;
      return seqNo;
  }
  
  // Read Genesis File Sequence to determine next seq no to use for output file
  private boolean updateFileSeqNo(int newSeqNo)
  {
      boolean result = true;
      if (openSQLServer())
      {          
          result = ssDB.updateSeqNo(genesisCustomer, servicePartner, country, profile, productId, newSeqNo);
          sqlConn = null;
      }
      // set to false if failed to connect to SQL Server
      else
          result = false;
      return result;
  }
  
  // Format seq no into 4 digit format with leading zeroes
  private String formatNextSeqNo(int nextSeqNo)
  {
      String result = "";
      if (nextSeqNo<10)
          result = "000"+nextSeqNo;
      else if (nextSeqNo<100)
          result = "00"+nextSeqNo;
      else if (nextSeqNo<1000)
          result = "0"+nextSeqNo;
      else 
          result = ""+nextSeqNo;      
      return result;
  }
  
  // opens SQL Server connection
  private boolean openSQLServer()
  {
      boolean result = true;
      // set up sql server connection for update
      String url = "jdbc:AvenirDriver://"+sqlServer+":1433/"+sqlServerDb;
      try
      {
        Class.forName("net.avenir.jdbcdriver7.Driver");
        sqlConn = DriverManager.getConnection(url,sqlServerUser,sqlServerPassword);
        ssDB = new sqlServerDB(sqlConn);
        //message = "   Connected to SQLServer";
        //writeToLogFile(message);
      }
      catch( Exception e)
      {
        message = "   Error opening SQLServer : " + e.toString();
        writeToLogFile(message);
        result = false;
      }
      return result;
  }
   
  // get supplied parameters from properties file
  private boolean propertiesSet()
  {
    boolean result = true;
    // get property values
    try
    {
      FileReader properties = new FileReader("passThroughPreProcessor.properties");
      BufferedReader buffer = new BufferedReader(properties);
      boolean eofproperties = false;
      String propline = buffer.readLine();
      String propname = propline.substring(0,8);
      int lineLength = propline.length();
      String propval = propline.substring(9,lineLength).trim();
      while (!eofproperties)
      {
    
        if (propname.equals("pThrType"))
          passThroughType = propval;
        if (propname.equals("genCust "))
          genesisCustomer = propval;
        if (propname.equals("servPart"))
          servicePartner = propval;
        if (propname.equals("country "))
          country = propval;
        if (propname.equals("profile "))
          profile = propval;
        if (propname.equals("prodId  "))
          productId = propval;
        if (propname.equals("sqlSServ"))
          sqlServer = propval;
        if (propname.equals("sqlSDB  "))
          sqlServerDb = propval;
        if (propname.equals("sqlSUser"))
          sqlServerUser = propval;
        if (propname.equals("sqlSPass"))
          sqlServerPassword = propval;
        if (propname.equals("logDir  "))
          logDir = propval;
        if (propname.equals("prePrDir"))
          preProcessDir = propval;
        if (propname.equals("procDir "))
          processedDir = propval;
        if (propname.equals("cdrDir  "))
          cdrDir = propval;
        if (propname.equals("rejDir  "))
          rejectDir = propval;
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
      result = false;
    }
    /*System.out.println(passThroughType);
    System.out.println(genesisCustomer);
    System.out.println(servicePartner); 
    System.out.println(countryCode); 
    System.out.println(profile);
    System.out.println(productId);
    System.out.println(sqlServer);
    System.out.println(sqlServerDb);
    System.out.println(sqlServerUser);
    System.out.println(sqlServerPassword);
    System.out.println(logDir);
    System.out.println(preProcessDir);
    System.out.println(cdrDir);*/
    return result;
  }
  
  // open cdr file
  private boolean openCDRFile(String cdrFileName)
  {
    cdrBW = null;
    boolean ok = true;
    try
    {
        cdrBW = new BufferedWriter(new FileWriter(cdrDir+File.separator+cdrFileName));
        // write header
        writeToCDRFile(HDR+COMMA+cdrFileName);
    }
    catch (IOException e)
    {
      System.out.println("   Error opening cdr file "+cdrFileName+" : "+e.toString());
      ok = false;
    }
      return ok;
  }  

  // write line to cdr file
  private void writeToCDRFile( String line )
  {
    try
    {
      cdrBW.write(line+"\r\n");
    }
    catch (IOException e)
    {
      System.out.println("   Error writing to cdr file : "+e.toString());
    }
  }

  // write line to cdr file
  private void closeCDRFile(String cdrFileName, int detailCount)
  {
    try
    {
      writeToCDRFile(TLR+COMMA+cdrFileName+COMMA+detailCount);  
      cdrBW.close();
    }
    catch (IOException e)
    {
      System.out.println("   Error closing cdr file : "+e.toString());
    }
  }
  
  // open log file
  private boolean openLogFile()
  {
    logBW = null;
    boolean ok = true;
    String logDate = new java.util.Date().toString();
    String logFilename
      = logDir+File.separator+"passThroughPreProcessor_"+
        logDate.substring(24,28)+decodeMonth(logDate.substring(4,7))+
        logDate.substring(8,10)+USCORE+logDate.substring(11,13)+
        logDate.substring(14,16)+logDate.substring(17,19)+
        "_log.txt";
    try
    {
      logBW = new BufferedWriter(new FileWriter(logFilename));
      message = 
        "Pass-Through Pre-Processing started at "+
        logDate.substring(24,28)+decodeMonth(logDate.substring(4,7))+
        logDate.substring(8,10)+logDate.substring(11,13)+
        logDate.substring(14,16)+logDate.substring(17,19);
      writeToLogFile(message);
      writeToLogFile("   ");
    }
    catch (IOException e)
    {
      System.out.println("   Error opening log file : "+e.toString());
      ok = false;
    }
    return ok;
  }

  // write line to log file
  private void writeToLogFile( String line )
  {
    try
    {
      logBW.write(line+"\r\n");
      System.out.println(line);
    }
    catch (IOException e)
    {
      System.out.println("   Error writing to log file : "+e.toString());
    }
  }

  // close log file
  private void closeLogFile()
  {
    String logDate = new java.util.Date().toString();
    message = 
        "Pass-Through Pre-Processing completed at "+
        logDate.substring(24,28)+decodeMonth(logDate.substring(4,7))+
        logDate.substring(8,10)+logDate.substring(11,13)+
        logDate.substring(14,16)+logDate.substring(17,19);
    writeToLogFile("   ");
    writeToLogFile(message);
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
    
}

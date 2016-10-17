package batch;

// required imports
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class NCRProcessing 
{ 
  // Message variables
  private String message, process;
  
  // Control constants
  private static String ALL = "ALL";
  private static String FILE = "FILE";
  private static String MARKUP = "MARKUP";
  private static String SINGLECHARGE = "SINGLECHARGE";
    
  // SQLServer environment variables
  private String sqlServer, sqlServerDb, sqlServerUser, sqlServerPassword;
  private Connection sqlConn;
  private sqlServerDB ssDB; 
  private boolean sqlServerOpen;
  
  // Directory variable
  private String logDir, dropDir, procDir, rejDir;  
  
  // File variables
  private BufferedWriter logBW;
  private File feedFile;
  
  // File line variables
  private String recType;
  private int trailerCount;
  private String[] lineColumn;
  private String lineItemDescription, lineItemReference, SN, startDate, startTime,
          endDate, endTime, siteId, NCRReference, chargeCurrency, chargeAmount,
          chargeType, chargeDescription;
      
  // String constants
  private static String USCORE = "_";
  private static String HYPHEN = "-";
  private static String SEMICOLON = ":";
  private static String SPACE = " ";
  private static String PERIOD = ".";
  private static String ZIP = "ZIP";
  private static String CSV = "csv";
  private static String VPIPE = "|";
  private static String TRAILER = "Trailer";
  private static String DETAIL = "Detail";
  
  // initialise supplied parameters
  private NCRProcessing()
  { 
    message="";
    process="";
    sqlServer="";
    sqlServerDb="";
    sqlServerUser="";
    sqlServerPassword="";
    logDir="";
    dropDir="";
    procDir="";
    rejDir="";
    recType = "";
    trailerCount=0;
    lineColumn = new String[14];
    sqlServerOpen=false;
  }    
  
  // main class links straight to control processing
  public static void main(String[] args)
  {
     String firstArg = "";
      if (args.length>0)
        firstArg = args[0];
    else
        firstArg = "ALL";
    NCRProcessing np = new NCRProcessing();
    np.control(firstArg);
  }  
  
  // controls NCR processing
  private void control(String firstParm)
  {
      
      // Check run mode is valid
      if ((firstParm.equals(ALL))||
          (firstParm.equals(FILE))||
          (firstParm.equals(MARKUP))||
          (firstParm.equals(SINGLECHARGE)))
      {
          
          if (propertiesSet())
          {
              if (openLogFile())
              {
                  writeToLogFile("Processes to run: "+firstParm);
                  writeToLogFile("   ");
                  // Decide which processes to run
                  if ((firstParm.equals(ALL))||(firstParm.equals(FILE)))
                      processFeedFiles();
                  if ((firstParm.equals(ALL))||(firstParm.equals(MARKUP)))
                      applyMarkups();
                  if ((firstParm.equals(ALL))||(firstParm.equals(SINGLECHARGE)))
                      singleChargeProcessing(); 
                  closeLogFile();
              }
          }
      }
      else
          System.out.println("Invalid mode "+firstParm+" selected");
  }
  
  // controls processing of NCR feed files
  private void processFeedFiles()
  {
      process = "Process Feed  Files";
      if (openSQLServer())
      {           
          writeToLogFile("Processing NCR Feed Files");
          //writeToLogFile("   ");
          // Process all feed files in the drop directory
          File dDir = new File(dropDir);
          File[] feedArray = dDir.listFiles();
          int fileCount=0, feedFileCount=0, rejectedFileCount=0;
          for (int i=0; i<feedArray.length; i++)
          {  
             // Process feed file
             feedFile = feedArray[i];
             String feedFileName = feedFile.getName();
             if (validNCRFile(feedFileName))
             {
                 feedFileCount++;
                 // move valid file to processed directory
                 File procFile = new File(procDir+File.separator+feedFileName);
                 if (!feedFile.renameTo(procFile))
                 {
                    message = "   IOERROR: Failed to move procesed file "+feedFileName+" to processed directory";
                    writeToLogFile(message);
                 }
             }
             else
             {
                 rejectedFileCount++;
                 // move invalid file to reject directory
                 File rejFile = new File(rejDir+File.separator+feedFileName);
                 if (!feedFile.renameTo(rejFile))
                 {
                    message = "   IOERROR: Failed to move rejected file "+feedFileName+" to rejected directory";
                    writeToLogFile(message);
                 }
             }
             fileCount++;       
          }
          if (fileCount==0)
          {
             writeToLogFile("   "); 
             writeToLogFile("   No NCR feed files to process");
          } 
          else
          {
              writeToLogFile("   ");
              writeToLogFile("   Files in drop directory: "+fileCount);
              writeToLogFile("      No. Files Processed: "+feedFileCount);
              writeToLogFile("      No. Files Rejected : "+rejectedFileCount);
          }
          closeSQLServer();          
      }
      writeToLogFile("   ");
  }
  
  // validates and process an NCR feed file
  private boolean validNCRFile(String filename)
  {
      writeToLogFile("   ");
      writeToLogFile("   Processing file : "+filename);
      boolean result = false;
      // Validate the file name
      // Check that there is a GSR, creation date and file count
      // and that the the filename ends .zip
      int namePartPos = 1;
      String namePart = "", GSR = "", creationDate = "", fileCount="";
      for (int i=0; i<filename.length(); i++)
      {
          String testChar = filename.substring(i,i+1);
          if ((testChar.equals(USCORE))||(testChar.equals(PERIOD)))
          {
              if (namePartPos==1)
                  GSR = namePart;
              if (namePartPos==2)
                  creationDate = namePart;
              if (namePartPos==3)
                  fileCount = namePart;
              namePartPos++;
              namePart = "";
          }
          else
              namePart = namePart + testChar;
      }
      String ext = filename.substring(filename.length()-3,filename.length());
      /*System.out.println(GSR);
      System.out.println(validGSR(GSR)); 
      System.out.println(creationDate);
      System.out.println(validCreationDate(creationDate));
      System.out.println(fileCount);
      System.out.println(validFileCount(fileCount));
      System.out.println(ext);*/
      if ((validGSR(GSR))&&
          (validCreationDate(creationDate))&&
          (validFileCount(fileCount))&&
          (ext.toUpperCase().equals(ZIP)))
      {
          // get expected next sequence no for the GSR and check that it 
          // matches the sequence no in the filename
          int fileSeqNo = ssDB.getNextGSRSeqNo(GSR);
          if (fileSeqNo==Integer.parseInt(fileCount))
          {
              // write new NCR File record for the file
              String formattedCreationDate = 
                      creationDate.substring(0,4)+HYPHEN+
                      creationDate.substring(4,6)+HYPHEN+
                      creationDate.substring(6,8);
              if (ssDB.insertNCRFile(GSR, fileSeqNo, formattedCreationDate))
              {
                  // check that internal csv file is same as zip file name and is only file
                  String expectedName = filename.substring(0, filename.length() - 3) + CSV;
                  String actualName = "", workPathname ="";
                  int noInternalFiles = 0;
                  try
                  {
                      ZipInputStream zis = new ZipInputStream(new FileInputStream(feedFile));
                      ZipEntry zisEntry = null;
                      while((zisEntry=zis.getNextEntry())!=null)
                      {
                        actualName = zisEntry.getName();
                        workPathname = dropDir+File.separator+actualName;
                        OutputStream out = new FileOutputStream(workPathname);
                        byte[] buf = new byte[1024];
                        int len;
                        while ((len = zis.read(buf)) > 0)
                        {
                            out.write(buf, 0, len);
                        }
                        out.close();
                        noInternalFiles++;
                      }
                      zis.closeEntry();
                      zis.close();
                  }
                  catch(java.io.IOException ex)
                  {
                      writeToLogFile("   IOERROR: Processing zip file "+filename+" : "+ex.getMessage());
                  }
                  if ((noInternalFiles==1)&&(expectedName.toUpperCase().equals(actualName.toUpperCase())))
                  {
                      File csvFile = new File(workPathname);
                      // process csv file
                      boolean goodFile = true, trailerProcessed = false;
                      int recCount = 0;
                      try
                      {
                        BufferedReader ffBR = new BufferedReader(new FileReader(new File(workPathname)));
                        String ffLine = ffBR.readLine();
                        while ((ffLine!=null)&&(goodFile))
                        {
                            //System.out.println(ffLine);
                            if (!validNCRLine(ffLine,GSR,creationDate,fileCount))
                                goodFile = false;
                            else
                            {
                                // check that trailer count matches record count 
                                if (recType.equals(TRAILER))
                                {
                                    trailerProcessed = true;
                                    if (trailerCount!=recCount)
                                    {
                                        goodFile = false;
                                        writeToLogFile(
                                            "      Trailer count of "+
                                            trailerCount+
                                            " does not match actual record count of "+
                                            recCount);
                                    }
                                }
                                else
                                {
                                    // add NCR Charge
                                    if (trailerProcessed)
                                    {
                                        goodFile = false;
                                        writeToLogFile("      Detail records found after trailer");
                                    }
                                    else
                                    {
                                        if (!ssDB.insertNCRCharge(
                                                GSR, 
                                                fileSeqNo, 
                                                recCount+1, 
                                                lineItemDescription, 
                                                lineItemReference, 
                                                SN, 
                                                startDate, 
                                                startTime, 
                                                endDate, 
                                                endTime, 
                                                siteId, 
                                                NCRReference, 
                                                chargeCurrency, 
                                                chargeAmount, 
                                                chargeType, 
                                                chargeDescription))
                                        {
                                            goodFile = false;
                                            writeToLogFile("      Failed to insert into NCR_charge for record seq no "+recCount);
                                        }
                                    }
                                }                                                              
                            }
                            ffLine = ffBR.readLine();
                            recCount++;
                        }
                        ffBR.close();
                      }
                      catch (java.io.IOException ex)
                      {
                          goodFile = false;
                          writeToLogFile("   IOERROR: Processing file "+filename+" "+ex.getMessage());
                      }
                      if (!trailerProcessed)
                      {
                          goodFile = false;
                          writeToLogFile("      No trailer found");
                      }
                      if (goodFile)
                      {
                        // as file has been successfully processed update NCR File                           
                        recCount--; // take trailer off record count
                        writeToLogFile("      "+recCount+" NCR Charges loaded");
                        if (ssDB.updateNCRFile(GSR, fileSeqNo, recCount))  
                        {                            
                            // finish by deleting work extracted csv
                            if (!csvFile.delete())
                              writeToLogFile("   IOERROR: Failed to delete extracted csv file "+actualName);
                            else
                              result = true;
                        }
                        else
                        {
                          writeToLogFile(
                              "   DBERROR: Failed to update NCR File for  "+
                                  filename+
                                  " to record count of"+
                                  recCount);  
                        }
                      }
                      else
                      {
                          // if bad file then delete any NCR charges created and NCR file
                          if (!ssDB.deleteNCRFile(GSR, fileSeqNo))
                              writeToLogFile("   DBERROR: Failed to delete NCR_File and any NCR_Charge for "+filename);
                          // finish by deleting work extracted csv
                          if (!csvFile.delete())
                              writeToLogFile("   IOERROR: Failed to delete extracted csv file "+actualName);
                      } 
                  }
                  else
                  {    
                      if (noInternalFiles==1)
                          writeToLogFile("   ZIPERROR: Expected csv file "+expectedName+" not found");
                      else
                          writeToLogFile("   ZIPERROR: No file or more than one file found in ZIP file");
                      // Delete any NCR charges created and NCR File
                      if (!ssDB.deleteNCRFile(GSR, fileSeqNo))
                          writeToLogFile("   DBERROR: Failed to delete NCR_File and any NCR_Charge for "+filename);                    
                  }   
              }
              else
              {
                  writeToLogFile("   DBERROR: Failed to insert NCR_File for "+filename);
              }
          }
          else
          {
              writeToLogFile(
                "   VALIDATIONERROR: Expected file sequence no of "+
                fileSeqNo+  
                " for GSR "+
                GSR+
                " does not match to that in file "+filename);
          }         
      }   
      else
          writeToLogFile("   File "+filename+" not in expected naming format");
      return result;
  }
  
  private boolean validNCRLine( 
      String NCRLine, 
      String GSR,
      String creationDate,
      String fileSeqNo)
  {
      boolean result = true, tooManyColumns = false;
      //System.out.println(NCRLine);
      // start by populating line column array with empty strings
      lineColumn[0] = "";
      lineColumn[1] = "";
      lineColumn[2] = "";
      lineColumn[3] = "";
      lineColumn[4] = "";
      lineColumn[5] = "";
      lineColumn[6] = "";
      lineColumn[7] = "";
      lineColumn[8] = "";
      lineColumn[9] = "";
      lineColumn[10] = "";
      lineColumn[11] = "";
      lineColumn[12] = "";
      lineColumn[13] = "";
      // move data from raw line into the output columns
      String workColumn = "";
      int columnPos = 0;
      for ( int i = 0; i < NCRLine.length(); i++)
      {
          String testChar = NCRLine.substring(i, i+1);
          if (testChar.equals(VPIPE))
          {
              if (columnPos>13)
                tooManyColumns = true;
              else
              {
                lineColumn[columnPos] = workColumn;
                workColumn = "";
                columnPos++;
              }
          }
          else
              workColumn = workColumn + testChar;
      }
      // deal with last column in line
      if (columnPos>13)
        tooManyColumns = true;
      else
        lineColumn[columnPos] = workColumn;
      // if not too many columns then determine if trailer or detail
      // and validate corect number of columns for data type
      if (tooManyColumns)
      {
        result = false;
        writeToLogFile("      File line has too many columns");
        writeToLogFile(NCRLine);
      }
          
      else
      {
          // If a trailer then the line should have a columns matching 
          // to the file creation date in column 1, column 0 should be the GSR,
          // column 2 should match file sequence no and column 3 should be 
          // record count
          // if a detail record same as trailer except column 1 should be
          // line item description and columns 3 to 13 should be populated
          if (lineColumn[1].equals(creationDate))
          {
              recType = TRAILER;
              if ((!lineColumn[0].trim().equals(GSR))||
                  (!lineColumn[2].trim().equals(fileSeqNo)) ||
                  (!stringIsInt(lineColumn[3].trim())))
              {
                result = false;    
                writeToLogFile("      Trailer incorrectly formatted");
              }                      
              else
                 trailerCount = Integer.parseInt(lineColumn[3]);
              if ((!lineColumn[4].trim().equals(""))||
                  (!lineColumn[5].trim().equals(""))||
                  (!lineColumn[6].trim().equals(""))||
                  (!lineColumn[7].trim().equals(""))||
                  (!lineColumn[8].trim().equals(""))||
                  (!lineColumn[9].trim().equals(""))||
                  (!lineColumn[10].trim().equals(""))||
                  (!lineColumn[11].trim().equals(""))||
                  (!lineColumn[12].trim().equals(""))||
                  (!lineColumn[13].trim().equals("")))
              {
                result = false;    
                writeToLogFile("      Trailer has extra data");                
              }
          }
          else
          {
              recType = DETAIL;
              if (!lineColumn[0].trim().equals(GSR))
              {
                result = false;    
                writeToLogFile("      Detail record does not match GSR");
                writeToLogFile(NCRLine);
              }                      
              else
              {
                  if ((lineColumn[1].trim().length()==0)||
                      (lineColumn[2].trim().length()==0)||
                      (lineColumn[3].trim().length()==0)||
                      (lineColumn[4].trim().length()==0)||
                      (lineColumn[5].trim().length()==0)||
                      (lineColumn[6].trim().length()==0)||
                      (lineColumn[7].trim().length()==0)||
                      (lineColumn[8].trim().length()==0)||
                      (lineColumn[9].trim().length()==0)||
                      (lineColumn[10].trim().length()==0)||
                      (lineColumn[11].trim().length()==0)||
                      (lineColumn[12].trim().length()==0)||
                      (lineColumn[13].trim().length()==0))
                  {
                    result = false;    
                    writeToLogFile("      Detail record missing expected data");
                    writeToLogFile(NCRLine);
                  }
                  else
                  {
                     lineItemDescription = lineColumn[1].trim(); 
                     lineItemReference = lineColumn[2].trim();  
                     SN = lineColumn[3].trim();  
                     startDate = lineColumn[4].trim();  
                     startTime = lineColumn[5].trim(); 
                     endDate = lineColumn[6].trim();  
                     endTime = lineColumn[7].trim();  
                     siteId  = lineColumn[8].trim();  
                     NCRReference  = lineColumn[9].trim();  
                     chargeCurrency  = lineColumn[10].trim();  
                     chargeAmount  = lineColumn[11].trim(); 
                     chargeType = lineColumn[12].trim();  
                     chargeDescription = lineColumn[13].trim();  
                  }
              }
          }              
      }
      return result;
  }
  
  // validates GSR string in filename
  private boolean validGSR(String gsr)
  {
      // check that GSR has three parts separated by hyphens
      boolean result = false;
      int gsrPartPos = 1;
      String gsrPart = "", gsrPart1 = "", gsrPart2 = "", gsrPart3 = "";
      for (int i=0; i<gsr.length(); i++)
      {
          String testChar = gsr.substring(i,i+1);
          if (testChar.equals(HYPHEN))
          {
              if (gsrPartPos==1)
                  gsrPart1 = gsrPart;
              if (gsrPartPos==2)
                  gsrPart2 = gsrPart;
              gsrPartPos++;
              gsrPart = "";
          }
          else
              gsrPart = gsrPart + testChar;
      }
      gsrPart3 = gsrPart;      
      if ((gsrPart1.length()>0)&&
          (gsrPart2.length()>0)&&
          (gsrPart3.length()>0)&&
          (stringIsInt(gsrPart3)))
          result = true;
      return result;
  }
  
  // validates creation date string in filename
  private boolean validCreationDate( String cDate)
  {
      // check that creation date is in yyyymmdd format
      // not checking that day value is correct for month value!
      boolean result = true;
      // check length of creation date is 8
      if (cDate.length()!=8)
          result = false;
      // check century value is 20
      if (result)
        if (!cDate.substring(0,2).equals("20"))
            result = false;  
      // check year value is numeric
      if (result)
        if (!stringIsInt(cDate.substring(2,4)))
            result = false; 
      // check month value is numeric
      if (result)
        if (!stringIsInt(cDate.substring(4,6)))
            result = false;
      // check month value is 1-12
      if (result)
        if (Integer.parseInt(cDate.substring(4,6))>12)
            result = false;
      // check day value is numeric
      if (result)
        if (!stringIsInt(cDate.substring(6,8)))
            result = false;
      // check day value is 1-31
      if (result)
        if (Integer.parseInt(cDate.substring(6,8))>31)
            result = false;
      return result;
  } 
    
  // validates four character filcount in filename
  private boolean validFileCount( String fCount)
  {
      boolean result = true;
      // filecount must be four characters long and numeric
      if ((fCount.length()!=4)||(!stringIsInt(fCount)))
          result = false;
      return result;
  }
  
  // checks if a string can be converted to an integer value
  private boolean stringIsInt ( String testInt)
  {
      boolean result = true;
      try
      {
        int value = Integer.parseInt(testInt);
      }
      catch(NumberFormatException e)
      {
        result = false;
      }
      return result;
  }
          
  // applys PPC and markup data to NCR charges
  private void applyMarkups()
  {
      process = "Mark-Up Processing";
      if (openSQLServer())
      {
          writeToLogFile("Applying Mark-Ups");
          writeToLogFile("   ");
          boolean ok = true;
          // Update Global Customer Id from the GSR
          int noGSRsMissing = ssDB.identifyMissingGSRs();
          writeToLogFile("   Updating any missing Global Customer Ids");
          if (noGSRsMissing>0)
            writeToLogFile(
                "      "+
                noGSRsMissing+
                " NCR Charge(s) rejected as GSRs not found");
          else if (noGSRsMissing==-9999)
          {
            writeToLogFile(
                "      DBERROR: Identify_Missing_GSRs : Unexpected error");
            ok = false;
          }
          else if (noGSRsMissing==-1)
          {            
            writeToLogFile(
                "      DBERROR: Identify_Missing_GSRs : "+
                "Failed updating NCR Charges with GSRs not found");
            ok = false;  
          }
          else if (noGSRsMissing==-2)
          {           
            writeToLogFile(
                "      DBERROR: Identify_Missing_GSRs : "+
                "Failed updating NCR Charges with GCID ");
            ok = false;  
          }          
          writeToLogFile("   ");
          // Update PPC Code from NCR PPC Mapping
          if (ok)
          {              
              writeToLogFile("   Updating any missing PPC Codes");
              int noPPCsMissing = ssDB.updatePPCCodes();
              if (noPPCsMissing>0)
                writeToLogFile(
                    "      "+
                    noPPCsMissing+
                    " NCR Charge(s) rejected as cannot determine PPC Code ");
              else if (noPPCsMissing==-9999)
              {
                writeToLogFile(
                    "      DBERROR: Update_PPC_Code : Unexpected error");
                ok = false;
              }
              else if (noPPCsMissing==-1)
              {            
                writeToLogFile(
                    "      DBERROR: Update_PPC_Code : "+
                    "Failed rejecting NCR Charges where PPC Code cannot be determined");
                ok = false;  
              }
              else if (noPPCsMissing==-2)
              {           
                writeToLogFile(
                    "      DBERROR: Update_PPC_Code : "+
                    "Failed updating NCR Charges with PPC Codes");
                ok = false;  
              }         
              writeToLogFile("   ");  
          }
          // Markup NCR Charges
          if (ok)
          {          
              writeToLogFile("   Applying any missing mark ups");
              int noMarkedUpBefore = ssDB.getMarkUpCount();
              int noFailedMarkups = ssDB.markupNCRCharge();
              if (noFailedMarkups>=0)
              {
                if (noFailedMarkups>0)
                    writeToLogFile(
                        "      "+
                        noFailedMarkups+
                        " NCR Charge(s) could not be marked up");
                int noMarkedUp = ssDB.getMarkUpCount() - noMarkedUpBefore;
                if (noMarkedUp>0)
                    writeToLogFile(
                        "      "+
                        noMarkedUp+
                        " NCR Charge(s) marked up");
              } 
              else if (noFailedMarkups==-9999)
              {
                writeToLogFile(
                    "      DBERROR: Markup_NCR_Charge : Unexpected error");
                ok = false;
              }
              else if (noFailedMarkups==-1)
              {            
                writeToLogFile(
                    "      DBERROR: Markup_NCR_Charge : "+
                    "Failed identifying NCR Charges not marked up");
                ok = false;  
              }
              else if (noFailedMarkups==-2)
              {           
                writeToLogFile(
                    "      DBERROR: Markup_NCR_Charge : "+
                    "Failed marking up NCR Charges");
                ok = false;  
              }         
              writeToLogFile("   ");  
          }
          closeSQLServer();
      }
  }
  
  // creates and updates single charges form marked up NCR charge data
  private void singleChargeProcessing()
  {
      process = "Single Charge Processing";
      if (openSQLServer())
      {
          writeToLogFile("Creating or updating Single Charges");
          writeToLogFile("   ");
          boolean ok = true;
          // Update Account Id from the Site Id
          int noSitesMissing = ssDB.getSiteAccounts();
          writeToLogFile("   Updating any missing Account Ids");
          if (noSitesMissing>0)
            writeToLogFile(
                "      "+
                noSitesMissing+
                " NCR Charge(s) rejected as Sites not found");
          else if (noSitesMissing==-9999)
          {
            writeToLogFile(
                "      DBERROR: Get_Site_Accounts : Unexpected error");
            ok = false;
          }
          else if (noSitesMissing==-1)
          {            
            writeToLogFile(
                "      DBERROR: Get_Site_Accounts : "+
                "Failed updating NCR Charges with Sites not found");
            ok = false;  
          }
          else if (noSitesMissing==-2)
          {           
            writeToLogFile(
                "      DBERROR: Get_Site_Accounts : "+
                "Failed updating NCR Charges with Account Id ");
            ok = false;  
          }  
          //  Validate Account Ids
          if (ok)
          {   
              int noInvalidAccountIds = ssDB.validateSiteAccounts();
              if (noInvalidAccountIds>0)
                writeToLogFile(
                    "      "+
                    noInvalidAccountIds+
                    " NCR Charge(s) rejected as Site Account does not exist ");
              else if (noInvalidAccountIds==-9999)
              {
                writeToLogFile(
                    "      DBERROR: Validate_Site_Accounts : Unexpected error");
                ok = false;
              }
              else if (noInvalidAccountIds==-1)
              {            
                writeToLogFile(
                    "      DBERROR: Validate_Site_Accounts : "+
                    "Failed rejecting NCR Charges where Sie Account Id invalid");
                ok = false;  
              }
              else if (noInvalidAccountIds==-2)
              {           
                writeToLogFile(
                    "      DBERROR: Validate_Site_Accounts : "+
                    "Failed updating NCR Charges with valid Site Account Ids");
                ok = false;  
              }         
              writeToLogFile("   ");  
          }
          
          //  Create single charges
          if (ok)
          {   
              int retValue = ssDB.summariseNCRCharges();
              if (retValue==0)
                writeToLogFile(
                    "   No NCR Summary Single Charges created");
              else if (retValue>0)                  
                writeToLogFile(
                    "   " + retValue+ "NCR Summary Single Charges created");
              else if (retValue==-2)
              {
                writeToLogFile(
                    "   Cannot summarise NCR charges with bills for at least one relevant customer running");
                ok = false;
              }
              else if (retValue<0)
              {            
                writeToLogFile("   DBERROR: Summarise_NCR_Charges : Return Code "+retValue);
                ok = false;  
              }       
              writeToLogFile("   ");  
          }
          closeSQLServer();
      }
  }
  
  // get supplied parameters from properties file
  private boolean propertiesSet()
  {
    boolean result = true;
    // get property values
    try
    {
      FileReader properties = new FileReader("NCRProcessing.properties");
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
        if (propname.equals("logDir  "))
          logDir = propval;
        if (propname.equals("dropDir "))
          dropDir = propval;
        if (propname.equals("procDir "))
          procDir = propval;
        if (propname.equals("rejDir  "))
          rejDir = propval;
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
    /*System.out.println(sqlServer);
    System.out.println(sqlServerDb);
    System.out.println(sqlServerUser);
    System.out.println(sqlServerPassword);
    System.out.println(logDir);
    System.out.println(dropDir);
    System.out.println(procDir);
    System.out.println(rejDir);*/
    return result;
  }
  
  // open log file
  private boolean openLogFile()
  {
    logBW = null;
    boolean ok = true;
    String logDate = new java.util.Date().toString();
    String logFilename
      = logDir+File.separator+"NCRProcessing"+USCORE+
        logDate.substring(24,28)+decodeMonth(logDate.substring(4,7))+
        logDate.substring(8,10)+USCORE+logDate.substring(11,13)+
        logDate.substring(14,16)+logDate.substring(17,19)+
        "_log.txt";
    try
    {
      logBW = new BufferedWriter(new FileWriter(logFilename));
      message = 
        "NCR Processing started at "+
        logDate.substring(24,28)+HYPHEN+
        decodeMonth(logDate.substring(4,7))+HYPHEN+
        logDate.substring(8,10)+SPACE+
        logDate.substring(11,13)+SEMICOLON+
        logDate.substring(14,16)+SEMICOLON+
        logDate.substring(17,19);
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
      if ((sqlServerOpen)&&(!line.trim().equals("")))
          if (line.substring(0,1).equals(SPACE))
            ssDB.insertNCRProcessingLog(process, line.substring(3, line.length()));
          else              
            ssDB.insertNCRProcessingLog(process, line);
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
        "NCR Processing completed at "+
        logDate.substring(24,28)+HYPHEN+
        decodeMonth(logDate.substring(4,7))+HYPHEN+
        logDate.substring(8,10)+SPACE+
        logDate.substring(11,13)+SEMICOLON+
        logDate.substring(14,16)+SEMICOLON+
        logDate.substring(17,19);
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
        message = "   Connected to SQLServer";
        sqlServerOpen = true;
        //writeToLogFile(message);
      }
      catch( Exception e)
      {
        message = "   DBERROR: Opening SQLServer : " + e.toString();
        writeToLogFile(message);
        result = false;
      }
      return result;
  }
  
  // closes connection to SQL Server
  private void closeSQLServer()
  {
      try
      {
        sqlConn.close();  
        sqlServerOpen = false;
      }
      catch( Exception e)
      {
        message = "   DBERROR: Closing SQLServer : " + e.toString();
        writeToLogFile(message);
      }
      
  }
  
}

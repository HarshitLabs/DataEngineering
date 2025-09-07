CREATE PROCEDURE [Medical].[usp_NewLoadDTToMaster](            
@MasterTable VARCHAR(100)              
,@Monthyear Varchar(100)=''            
)            
AS            
BEGIN  
  IF (@MasterTable='NewPatientMaster')                  
  BEGIN                  
            
  --Update existing             
  UPDATE A            
  SET            
  A.DateofBirth  = TRY_CAST(ISNULL(B.DateofBirth,'') AS DATE)        
  ,A.Street1  = ISNULL(TRIM(B.Street1),'')            
  ,A.Street2  = ISNULL(TRIM(B.Street2),'')            
  ,A.City   = ISNULL(TRIM(B.City),'')            
  ,A.State   = ISNULL(TRIM(B.State),'')            
  ,A.Zip   = ISNULL(TRIM(B.Zip),'')            
  ,A.Phone1   = ISNULL(TRIM(B.Phone1),'')            
  ,A.Phone2   = ISNULL(TRIM(B.Phone2),'')            
  ,A.Gender   = ISNULL(B.Gender,'')            
  ,A.Email   = ISNULL(TRIM(B.Email),'')            
  ,A.RefSource      = ISNULL(B.RefSource,'')            
  ,A.SubRefSource   = ISNULL(B.SubRefSource,'')             
  ,A.Store   = ISNULL(TRIM(B.Store),'')            
  from [Medical].[NewPatientMaster]  A            
  INNER JOIN Medical.NewPatientsModified_DT B            
  ON  A.CustomerID    =ISNULL(TRIM(B.CustomerID),'')            
  AND A.LastName          =ISNULL(TRIM(B.LastName),'')            
  AND A.FirstName         =ISNULL(TRIM(B.FirstName),'')   
  Where             
   A.DateofBirth != TRY_CAST(ISNULL(B.DateofBirth,'') AS DATE)             
  OR A.Street1  != ISNULL(TRIM(B.Street1),'')            
  OR A.Street2  != ISNULL(TRIM(B.Street2),'')            
  OR A.City   != ISNULL(TRIM(B.City),'')            
  OR A.State  != ISNULL(TRIM(B.State),'')            
  OR A.Zip   != ISNULL(TRIM(B.Zip),'')            
  OR A.Phone1  != ISNULL(TRIM(B.Phone1),'')            
  OR A.Phone2  != ISNULL(TRIM(B.Phone2),'')            
  OR A.Gender  != ISNULL(B.Gender,'')            
  OR A.Email  != ISNULL(TRIM(B.Email),'')            
  OR A.RefSource    != ISNULL(B.RefSource,'')            
  OR A.SubRefSource != ISNULL(B.SubRefSource,'')             
  OR A.Store  != ISNULL(TRIM(B.Store),'')            
            
  --Insert New            
   INSERT INTO  [Medical].[NewPatientMaster]                  
   ( [CustomerID]             
 ,[CustomerIDName]            
 ,[Store]                  
 ,[LastName]                 
 ,[FirstName]                 
 ,[Gender]                 
 ,[DateofBirth]                
 ,[Street1]                 
 ,[Street2]                 
 ,[City]                  
 ,[State]                  
 ,[Zip]                  
 ,[Phone1]                 
 ,[Phone2]                 
 ,[Email]                  
 ,[LeftLoss]                 
 ,[RightLoss]                 
 ,[MailStatus]                
 ,[CallStatus]                
 ,[MaritalStatus]                
 ,[HIPAAWaiverSigned]               
 ,[RefSource]                 
 ,[SubRefSource]                
 ,[LastHearingAidPurchase]             
 ,[LastHearingTest]               
 ,[LastModified]                
 ,[SycleExportDate]               
 ,[FranchiseOwner]             
   )                  
   SELECT   DISTINCT                 
     ISNULL(TRIM(S.[CustomerID]),'')             
 ,ISNULL(TRIM(CONCAT(TRIM(S.[CustomerID]),' ',TRIM(S.[Lastname]),', ',TRIM(S.[Firstname]))),'')            
 ,ISNULL(TRIM(S.[Store]),'')                 
 ,ISNULL(TRIM(S.[LastName]),'')                
 ,ISNULL(TRIM(S.[FirstName]),'')                
 ,ISNULL(S.[Gender],'')        
 ,TRY_CAST(ISNULL(S.[DateofBirth],'') AS DATE)              
 ,ISNULL(TRIM(S.[Street1]),'')                
 ,ISNULL(TRIM(S.[Street2]),'')                
 ,ISNULL(TRIM(S.[City]),'')                 
 ,ISNULL(TRIM(S.[State]),'')                 
 ,ISNULL(TRIM(S.[Zip]),'')                 
 ,ISNULL(TRIM(S.[Phone1]),'')                
 ,ISNULL(TRIM(S.[Phone2]),'')                
 ,ISNULL(TRIM(S.[Email]),'')                 
 ,RIGHT(LEFT(ISNULL(REPLACE(TRIM(S.[LeftLoss]),'Did Not Test',''),''),7),1)             
 ,RIGHT(LEFT(ISNULL(REPLACE(TRIM(S.[RightLoss]),'Did Not Test',''),''),7),1)                
 ,ISNULL(S.[MailStatus],'')               
 ,ISNULL(S.[CallStatus],'')               
 ,ISNULL(S.[MaritalStatus],'')               
 ,ISNULL(S.[HIPAAWaiverSigned],'')              
 ,ISNULL(S.[RefSource],'')                
 ,ISNULL(S.[SubRefSource],'')               
 ,ISNULL(S.[LastHearingAidPurchase],'1900-01-01 00:00:00.000')            
 ,ISNULL(S.[LastHearingTest],'1900-01-01 00:00:00.000')              
    ,GETDATE()                  
    ,S.[SycleExportDate]              
 ,ISNULL(S.FranchiseOwner,'')            
    FROM Medical.NewPatientsModified_DT S                  
    LEFT JOIN [Medical].[NewPatientMaster] T                  
    ON  T.CustomerID  =ISNULL(TRIM(S.CustomerID),'')            
 AND T.LastName          =ISNULL(TRIM(S.LastName),'')            
 AND T.FirstName         =ISNULL(TRIM(S.FirstName),'')            
   WHERE T.CustomerID IS NULL                  
  END              
            
END
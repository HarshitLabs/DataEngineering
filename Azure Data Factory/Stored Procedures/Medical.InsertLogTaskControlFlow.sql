  
CREATE PROCEDURE Medical.InsertLogTaskControlFlow       
(      
@RunId NVARCHAR(255),      
@PipelineName NVARCHAR(255),      
@StartTime DATETIME =NULL,      
@DataInserted INT=NULL      
)      
AS      
 BEGIN      
  BEGIN TRY      
   BEGIN TRAN      
    INSERT INTO Medical.[LogTaskControlFlow]      
    (      
    RunId,      
    TableName,      
    StartTimeDW,      
    TotalRowsInsertedDW,      
    FeedStatusDW      
    )      
    SELECT      
    @RunId,      
    @PipelineName,      
    NULLIF(@StartTime,GETDATE()),      
    @DataInserted,      
    'In-progress'      
    
 SELECT LogTaskId  FROM Medical.[LogTaskControlFlow]  WHERE RunId=@RunId   AND TableName=@PipelineName    
   COMMIT TRAN      
  END TRY      
 BEGIN CATCH      
  IF @@TRANCOUNT>=1      
   ROLLBACK TRAN      
  INSERT INTO Medical.Logerror      
  (      
  ObjectName,      
  ErrorCode,      
  ErrorDescription,      
  ErrorGenerationTime      
  )      
  SELECT       
  ERROR_PROCEDURE() AS ErrorProcedure,      
  ERROR_NUMBER() AS ErrorNumber,      
  ERROR_MESSAGE() AS ErrorMessage,      
  GETDATE()      
 END CATCH      
END 
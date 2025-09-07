  
  
CREATE PROCEDURE Medical.UpdateLogTaskControlFlowFailure      
(      
@RunId NVARCHAR(255),      
@ErrorDescription NVARCHAR(MAX)      
)      
AS      
 BEGIN      
  BEGIN TRY      
   BEGIN TRAN      
    UPDATE Medical.[LogTaskControlFlow]      
    SET      
    FeedStatusDW='Failed',      
 EndTimeDW=GETDATE(),    
    ErrorDescription= ErrorDescription + ' | ' + @ErrorDescription      
    WHERE RunId=@RunId      
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
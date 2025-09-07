  CREATE PROCEDURE Medical.UpdateLogTaskControlFlow    
(    
@RunId NVARCHAR(255)    
)    
AS    
 BEGIN    
  BEGIN TRY    
   BEGIN TRAN    
    UPDATE Medical.[LogTaskControlFlow]    
    SET    
    FeedStatusDW='Completed',    
    EndTimeDW=GETDATE()    
    WHERE RunId=@RunId    
  
 SELECT LogTaskID FROM Medical.[LogTaskControlFlow]   
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
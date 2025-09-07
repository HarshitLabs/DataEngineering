CREATE TABLE Medical.LogError (
    ErrorID bigint IDENTITY(1,1) NOT NULL PRIMARY KEY,
    ObjectName nvarchar(1000) NULL,
    ErrorCode bigint NULL,
    ErrorDescription nvarchar(max) NULL,
    ErrorGenerationTime datetime NULL,
    IsModified bit NULL
);

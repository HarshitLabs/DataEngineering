CREATE TABLE Medical.LogTaskControlFlow (
    LogTaskID bigint NOT NULL PRIMARY KEY,
    RunId varchar(100) NULL,
    TableName varchar(100) NULL,
    SycleDate datetime NULL,
    DTTotalRows bigint NULL,
    DTTotalRowsPassed bigint NULL,
    DTTotalRowsFailed bigint NULL,
    StartTimeDW datetime NULL,
    EndTimeDW datetime NULL,
    TotalRowsInsertedDW bigint NULL,
    TotalRowsUpdatedDW bigint NULL,
    FeedStatusDW varchar(100) NULL,
    ErrorDescription varchar(max) NULL
);

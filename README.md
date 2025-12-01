# Supply-Chain-Management-Pipeline
Event-driven SCM pipeline using S3, Glue, Lambda, SNS, and EventBridge. Incoming ERP/CRM fact data triggers validation, schema checks, and incremental ETL into silver dimension/fact tables. Gold summaries are built next, and a final status email is sent after successful workflow completion.

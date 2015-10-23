# PikaDB User Guide

## Constraint Declarations

One of the primary features of PikaDB is the ability to mix schema enforced database columns with the flexibility of schemaless data. This is done by declaring your constraints on initialization. You can add constraints as you go - any existing data that does not conform will be removed and all future data will only be accepted if it lives up to the constraints.

```java
PikaDB db=new PikaDB("mydatafile.db");
Table user=db.declareTable("Users");
user.declareColumn("Id",INT,REQUIRED);
user.declareColumn("FirstName",STRING,REQUIRED);
user.declareColumn("LastName",STRING,REQUIRED);
user.declareColumn("ProfileURL",STRING);
```

You do not have to declare tables and columns before you use them a declaration is only needed if you want PikaDB to enforce a constraint.

```java
PikaDB db=new PikaDB("mydatafile.db");
Table user=db.declareTable("TimeSeries",PRESERVE_ORDER);
user.declareColumn("Id",INT,REQUIRED);
user.declareColumn("Value",NUMERIC,REQUIRED);
```

# RaptorDB Document Store

NoSql, JSON based, Document store database with compiled .net map functions and automatic hybrid bitmap indexing and LINQ query filters (now with standalone Server mode, Backup and Active Restore, Transactions, Server side queries, MonoDroid support, HQ-Branch Replication)

see the article here : [http://www.codeproject.com/Articles/375413/RaptorDB-the-Document-Store] (http://www.codeproject.com/Articles/375413/RaptorDB-the-Document-Store)

## Quick Start

First compile the source, then you can easily run any c# file like this:

```
# run any cs file
c:\rdb\test script> ..\tools\nscript.exe sample.cs

# or just run the batch file
c:\rdb\test script> run.cmd  
```

The `sample.cs` file now contains a comment section at the top for specifing references used which will tell `nscript.exe` where to find the dll files:

```
// ref : ..\output\raptordb.dll
// ref : ..\output\raptordb.common.dll
// ref : ..\faker.dll
using System;
using System.Collections.Generic;
...
```

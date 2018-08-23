---
name: Bug report
about: Create a report to help us improve

---
<!-- 
 Before raising a bug report please consider the following:
   1. If you want to ask a question don't raise a bug report - rather use the mailing list at https://groups.google.com/forum/#!forum/waggle-dance-user
   2. Please ensure that the bug your are reporting is actually in Waggle Dance and not with Hive or whatever tools you are using to communicate with the metastore. 
     Because Waggle Dance proxies the Hive metastore service it will just return any errors that the metastore itself throws and users sometimes mistakenly 
     report these as Waggle Dance issues. The easiest way to check this is to perform your operation against the underlying metastore directly 
     (i.e. remove Waggle Dance from the equation by setting "hive.metastore.uris" to your metastore service endpoint and not Waggle Dance). If the issue still 
     persists then it's not related to Waggle Dance so please don't report it here.  
-->
**Describe the bug**
A clear and concise description of what the bug is.

**To Reproduce**
Steps to reproduce the behaviour ideally including the configuration files you are using (feel free to rename any sensitive information like server and table names etc.)

**Expected behavior**
A clear and concise description of what you expected to happen.

**Logs**
Please add the log output from Waggle Dance when the error occurs, full stack traces are especially useful. It might also be worth looking in the Hive metastore 
service log files to see if anything is output there and including that too.

**Versions (please complete the following information):**
 - Waggle Dance Version: 
 - Hive Versions: for whatever Hive client libraries you are using as well as the versions of Hive in the metastore sevices that Waggle Dance is proxying

**Additional context**
Add any other context about the problem here.

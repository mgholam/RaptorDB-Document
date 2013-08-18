DATA Folder
     |
	 |- Replication > (branch mode)
	 |          |-: branch.dat
	 |          |
	 .          |- Inbox >
	            |      |-:  0000000n.mgdat.gz,  
				|
				|- Outbox >      


- if inbox contains : "0000000n.counter" then error occurred and the text is in "0000000n.error.txt"
- you can skip the offending document if you increment the "counter" file (when you can't overcome the exception)
- files will be downloaded to the inbox folder in branch mode
- "branch.dat" in the "Replication" folder stores counter information for replication 



DATA Folder
     |
	 |- Replication > (HQ mode)
	 |          |-: BranchName1.last
	 |          |
	 |          |- Inbox >
	 |          |      |
	 .          |      |- BranchName1 >
	            |      |         |-:   0000000n.mgdat.gz
	            |      |         |
				|
				|- Outbox >
				|      | 
				|      |- BranchName1 > 
				|      |- BranchName2 >


- if inbox contains : "0000000n.counter" then error occurred and the text is in "0000000n.error.txt"
- you can skip the offending document if you increment the "counter" file (when you can't overcome the exception)
- files will be downloaded to the inbox folder in branch mode



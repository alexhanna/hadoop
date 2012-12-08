hadoop dfs -rmr output
hadoop jar frequency.jar org.ahanna.Frequency elex2012-wave3 output \
-mode volume 
#-userFile elex-followlists/follow-r1.txt 
#-catFile elex-followlists/top-level-uid_cat.csv 

hadoop dfs -rmr lzo-output
hadoop jar LzoJsonWordCount.jar org.ahanna.LzoJsonWordCount -libjars /project/hanna/src/elephant-bird/build/elephant-bird-2.2.0.jar lzotest lzo-output

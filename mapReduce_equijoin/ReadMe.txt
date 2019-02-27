Distributed Database Systems Assignment4 ReadMe File. 


Execution Steps:
-------------------------------------------------------------------------------------------------------------------------------------------
Step 1: Compile the java code
Step 2: jar the .class files folder of the java program
Step 3: make Sure they are in the hadoop/bin folder
Step 4: Hadoop/bin folder should contain Inputfile.txt in a directory. e.g HDFSInputFile
Step 4: Run the below command to start hadoop program
	sudo -u ./hadoop jar <JarFile> ClassFileName <HDFSInputFile> <HDFSOutputFile>
Step 5: Observe in the HDFSOutpuFile Directory


Mapper Class:
-------------------------------------------------------------------------------------------------------------------------------------------
Mapper Implementation via Map method,processes Input in the form of a string as provided by specified TextInputFormat in Driver class.
Mapper emits intermediate key value pairs
Key will be the join column
Value will be lines with the given join column

Reducer Class:
-------------------------------------------------------------------------------------------------------------------------------------------
The Reducer Implementation, via the reduce method append the values, which are the occurence for each key.
Step 1: Create a string array to hold the list of values.
Step 2: Inorder not to append the tuples from the same table, I will perform a check on the tablename before every time I append the values.
Step 3: Finally using output.collect will write into the Output file part-00000.


Driver Class:
---------------------------------------------------------------------------------------------------------------------------------------------
Step 1: Created map-reduce job using JobConfig, the name of that job is equijoin
Step 2: Set the key class for Output data
Step 3: Set the value class for job outputs
Step 4: set the Mapper class for job
Step 5: set the Reducer class for job
Step 6: get the InputFilePath and OutputFilePath from the arguments given while executing
Step 7: set the InputFormat  implementation for the map-reduce job
Step 8: set the OutputFormat  implementation for the map-reduce job
Step 9: Finally, run the job to submit and monitor its progress.


--------------------------------------------------------------------------------------------------------------------------------------------
Sample Input:
R, 2, Don, Larson, Newark, 555-3221
S, 1, 33000, 10000, part1
S, 2, 18000, 2000, part1
R, 3, Sal, Maglite, Nutley, 555-6905
S, 3, 24000, 5000, part1
S, 4, 22000, 7000, part1
R, 4, Bob, Turley, Passaic, 555-8908



Output Sample:
S, 2, 18000, 2000, part1, R, 2, Don, Larson, Newark, 555-3221
S, 3, 24000, 5000, part1, R, 3, Sal, Maglite, Nutley, 555-6905
R, 4, Bob, Turley, Passaic, 555-8908, S, 4, 22000, 7000, part1


Author - Manasa Pola

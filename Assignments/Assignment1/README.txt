Jiadao Zou
jxz172230
Assignment1

1. download and unzip the homwork package

2. upload ass1-1.0.jar to SSH under your account

3. Problem 1:
	To run part1, execute: hadoop jar ass1-1.0.jar ass1_1.ass1_1 assignment1 (it will overwrite the files if already exist)
	To check: hdfs dfs -ls assignment1

4. Problem 2:
	To run part 2, execute: hadoop jar ass1-1.0.jar ass1_2.ass1_2 topic
	It will create a folder <topic-search> under your hdfs root directory.
	
	Prob:
	(There exist a wired problem: Exception in thread "main" java.lang.NoClassDefFoundError: twitter4j/TwitterException.
		However, when I run this code locally (ass1/ass1_2_out), it works fine. I am still working on it to see whether it is the version problem or other import problem.)
	That problem is solved. Now everything works fine!

	Details:
		topic:	economy
		time:  "2019-2-1",
                "2019-2-2",
                "2019-2-3",
                "2019-2-4",
                "2019-2-5",
                "2019-2-6",
        size:	for convinence, I only query 500 results for each day, which makes the 6 files around 3.4M in total, However, it is possible to query as much as possible as long as change one parameter.


Explaination:
(e.g.) Your target location could be /user/<NetID>/Assignment1/i for parti
	and /user/<NetID>/Assignment1/ii for partii for part 2 
	Or you can just make the location: xxx without /user/<NetID>
The folders under your target address should be already created





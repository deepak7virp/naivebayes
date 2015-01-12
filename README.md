# naivebayes
Naive Bayes Clustering
Instructions to run MapReduce

1.Copy .jar file into hduser directory.
2.Copy hw4data.csv into hduser
3.Create 5 directories in hdfs : input1,input2,input3,input4,input5
4. Copy file as :
	input1 : data2.txt, data3.txt, data4.txt, data5.txt
	input2 : data1.txt, data3.txt, data4.txt, data5.txt
	input3 : data1.txt, data2.txt, data4.txt, data5.txt
	input4 : data1.txt, data2.txt, data3.txt, data5.txt
	input5 : data1.txt, data2.txt, data3.txt, data4.txt
5. Run command : hadoop jar nbayes.jar MR_BAYES 

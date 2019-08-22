# ICP 1 Documentation 


![image](https://user-images.githubusercontent.com/27305718/63532448-546a3900-c4d0-11e9-8d30-e3e95987d4a0.png)
First I created the local file using vim. 

Then I ran    hdfs dfs -appendToFile localfile /user/hadoop/hadoopfile

From there I opened my cloudera manager and saw that HDFS was indeed running. 

![image](https://user-images.githubusercontent.com/27305718/63532468-5fbd6480-c4d0-11e9-95b3-57b21d0de6ca.png)
From there I figured out where I had downloaded the text data sets from  and then used the given command and put them into hadoop 

![image](https://user-images.githubusercontent.com/27305718/63532494-6d72ea00-c4d0-11e9-9ef1-5481103a4ba7.png)

From here I use the -appendToFile to move first file into hadoop and then append the second file to it. 


![image](https://user-images.githubusercontent.com/27305718/63532515-7794e880-c4d0-11e9-9864-b2b347f21fce.png)
From here I use the head and tail command to show the first 5 lines and the last 5 lines of the original dataset with the appended dataset. 


![image](https://user-images.githubusercontent.com/27305718/63532534-811e5080-c4d0-11e9-8180-0750cb936494.png)
**Visualizing the file in hue. 
![image](https://user-images.githubusercontent.com/27305718/63532814-233e3880-c4d1-11e9-8690-bc45f48a6435.png)
The original data sets.  
![image](https://user-images.githubusercontent.com/27305718/63532569-95fae400-c4d0-11e9-9a7e-5887cb80d543.png)
Making a 3rd File**

![image](https://user-images.githubusercontent.com/27305718/63532574-98f5d480-c4d0-11e9-8ea4-34b2c1233f80.png)
**Putting that file into Hadoop**

![image](https://user-images.githubusercontent.com/27305718/63532579-9b582e80-c4d0-11e9-991b-e4b2fcfff24a.png)
**Appending the file**

![image](https://user-images.githubusercontent.com/27305718/63532588-9eebb580-c4d0-11e9-92b2-d54ed87f0a05.png)
**Visulizing the file**

![image](https://user-images.githubusercontent.com/27305718/63532592-a14e0f80-c4d0-11e9-95c9-5e6952ea24ee.png)
**Appending the original files to the 3rd file in Hadoop.**
![image](https://user-images.githubusercontent.com/27305718/63532604-a612c380-c4d0-11e9-8f62-74d71721ac2e.png)

![image](https://user-images.githubusercontent.com/27305718/63532623-ad39d180-c4d0-11e9-8b49-a9b67e28e151.png)
As we can see the 3 data sets are now combined. As the 3rd data set is clearly visible in the first 3 lines then the amount of pages is the same as when we combined the first 2 data sets. 

// written by Connor Larson, Nathanael Goertzen, Micheal Homer. 
import java.io.IOException;
import java.util.StringTokenizer;
import java.io.*;
import java.util.*;
import java.lang.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {
    // Mapper class is below
    public static class FriendMapper
            extends Mapper<Object, Text, Text, Text> {
                // creates our variables. 
        private final static IntWritable one = new IntWritable(1);
        private Text user = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            // splits up the data based on parameters and stores it into arrays
            String[] linedata = value.toString().split("->");
            String friend1 = linedata[0];
            String[] friendlist = linedata[1].split(",");
            for (int i = 0; i< friendlist.length; i++ ){
                // compares the order of the pairs and writes the key value pair. 
                String friend2 = friendlist[i];
                if (friend1.charAt(0) < friend2.charAt(0)) {
                    user.set(friend1 + "," + friend2);
                }else{
                    user.set(friend2 + "," + friend1);
                }
                context.write(user, new Text(linedata[1]));
            }

        }
    }
    // Reducer class is below
    public static class FriendReducer
            extends Reducer<Text, Text, Text, Text> {
                // variable creation 
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            Set<String> set1 = new HashSet<String>();
            // loops through the values. 
            // If the set is empty it adds it to the set
            // If not it does a .retainAll() to do set intersection to only keep the ones that are in both sets. 
            for (Text friends: values){
                 String[] temp =friends.toString().split(",");
                if (set1.isEmpty()){
                    set1.addAll(Arrays.asList(temp));
                }else {
                    Set<String> set2 = new HashSet<String>();
                    set2.addAll(Arrays.asList(temp));
                    set1.retainAll(set2);
                }
            }
            // converts the Set to a string and writes the result to the output file.
            Object[] joinedList= set1.toArray();
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i< joinedList.length; i++){
                sb.append( joinedList[i] + "," );
            }
            result.set(new Text(sb.toString()));
            context.write(key,result);
        }
    }
    // main class that assigns jobs and runs our above functions. 
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(FriendMapper.class);
        job.setCombinerClass(FriendReducer.class);
        job.setReducerClass(FriendReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
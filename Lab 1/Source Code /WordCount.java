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

    public static class FriendMapper
            extends Mapper<Object, Text, Text, Text> {
        private final static IntWritable one = new IntWritable(1);
        private Text user = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] linedata = value.toString().split("->");
            String friend1 = linedata[0];
            String[] friendlist = linedata[1].split(",");
            for (int i = 0; i< friendlist.length; i++ ){
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

    public static class FriendReducer
            extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            Set<String> set1 = new HashSet<String>();
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
            Object[] joinedList= set1.toArray();
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i< joinedList.length; i++){
                sb.append( joinedList[i] + "," );
            }
            result.set(new Text(sb.toString()));
            context.write(key,result);
        }
    }

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
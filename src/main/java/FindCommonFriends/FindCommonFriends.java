package FindCommonFriends;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Random;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.WritableComparable;


public class FindCommonFriends {

  public static class ReverseMapper
       extends Mapper<Object, Text, Text, Text>{
    @Override
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
        String line = value.toString();

        String[] userAndfriends = line.split(",");      
        String user = userAndfriends[0];        //获取输入值中的用户名称
        String[] friends = userAndfriends[1].split(" ");        //获取输入值中的好友列表
        for (String friend : friends) {
            context.write(new Text(friend), new Text(user));
        }
        // StringTokenizer itr = new StringTokenizer(line,",");
        // String person = itr.nextToken();
        // String friends = itr.nextToken();
        // StringTokenizer itrFriends = new StringTokenizer(friends);
        // while (itrFriends.hasMoreTokens()) {
        //     String friend = itrFriends.nextToken();
        //     context.write(new Text(friend), new Text(person));
        // }
    }
  }

  public static class ReverseReducer
       extends Reducer<Text,Text,Text,Text> {
      public void reduce(Text key, Iterable<Text> values,
                        Context context) throws IOException, InterruptedException {
        String people = new String();
        for (Text val : values) {
          people += val.toString();
        }
        context.write(key, new Text(people));
      }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
    String[] remainingArgs = optionParser.getRemainingArgs();
    if (remainingArgs.length != 2) {
      System.err.println("Usage: FindCommonFriends <in> <out>");
      System.exit(2);
    }
    Job job = Job.getInstance(conf, "find common friends");
    job.setJarByClass(FindCommonFriends.class);
    job.setMapperClass(ReverseMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(ReverseReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    List<String> otherArgs = new ArrayList<String>();
    for (int i=0; i < remainingArgs.length; ++i) {
      otherArgs.add(remainingArgs[i]);
    }
    FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));

    // Path tempDir = new Path("tmp-" + Integer.toString(  
    //         new Random().nextInt(Integer.MAX_VALUE))); //第一个job的输出写入临时目录
    FileOutputFormat.setOutputPath(job, new Path(otherArgs.get(1)));
    //FileOutputFormat.setOutputPath(job, tempDir);

    //job.setOutputFormatClass(SequenceFileOutputFormat.class);

    System.exit(job.waitForCompletion(true) ? 0 : 1);
   

  //   if(job.waitForCompletion(true))  
  //   {  
  //       //新建一个job处理排序和输出格式
  //       Job sortJob = new Job(conf, "sort");  
  //       sortJob.setJarByClass(WordCount.class);  

  //       FileInputFormat.addInputPath(sortJob, tempDir); 

  //       sortJob.setInputFormatClass(SequenceFileInputFormat.class);  
        
  //       //map后交换key和value
  //       sortJob.setMapperClass(InverseMapper.class);  
  //       sortJob.setReducerClass(SortReducer.class);
        
  //       FileOutputFormat.setOutputPath(sortJob, new Path(otherArgs.get(1)));  

  //       sortJob.setOutputKeyClass(IntWritable.class);  
  //       sortJob.setOutputValueClass(Text.class); 

  //       //排序改写成降序
  //       sortJob.setSortComparatorClass(IntWritableDecreasingComparator.class);  

  //       System.exit(sortJob.waitForCompletion(true) ? 0 : 1); 
  //   }  

  //   FileSystem.get(conf).deleteOnExit(tempDir);
  }
}


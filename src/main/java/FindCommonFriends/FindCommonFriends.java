package FindCommonFriends;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Random;
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;

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
        StringTokenizer itr = new StringTokenizer(line,",");
        String person = itr.nextToken();
        String friends = itr.nextToken();
        StringTokenizer itrFriends = new StringTokenizer(friends);
        while (itrFriends.hasMoreTokens()) {
            String friend = itrFriends.nextToken();
            context.write(new Text(friend), new Text(person));
        }
    }
  }

  public static class ReverseReducer
       extends Reducer<Text,Text,Text,Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values,
                      Context context) throws IOException, InterruptedException {
      String people = new String();
      ArrayList<String> strValues = new ArrayList();
      for (Text val : values) {
        strValues.add(val.toString());
        // people += val.toString();
      }
      people = String.join(",", strValues);
      context.write(key, new Text(people));
    }
  }

  public static class PairMapper
       extends Mapper<Object, Text, Text, Text>{
    @Override
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer itr = new StringTokenizer(line,"\t");
        String friend = itr.nextToken();
        String[] people = itr.nextToken().split(",");
        Arrays.sort(people); 
        for (int i = 0; i < people.length - 1; i++) {    
          for (int j = i + 1; j < people.length; j++) {
            context.write(new Text("[" + people[i] + "," + people[j] + "],"), new Text(friend));
          }
        }
    }
  }

  public static class PairReducer
       extends Reducer<Text,Text,Text,NullWritable> {
    @Override
    public void reduce(Text key, Iterable<Text> friends,
                      Context context) throws IOException, InterruptedException {
      String commonFriends = new String("[");
      ArrayList<String> strValues = new ArrayList();
      for (Text friend : friends) {
        strValues.add(friend.toString());
      }
      commonFriends += String.join(",", strValues);
      commonFriends += "]";
      context.write(new Text("("+key.toString()+commonFriends+")"), NullWritable.get());
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
    job.setReducerClass(ReverseReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    List<String> otherArgs = new ArrayList<String>();
    for (int i=0; i < remainingArgs.length; ++i) {
      otherArgs.add(remainingArgs[i]);
    }
    FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));

    Path tempDir = new Path("tmp-" + Integer.toString(  
            new Random().nextInt(Integer.MAX_VALUE))); //第一个job的输出写入临时目录
    Path outputPath = new Path(otherArgs.get(1));
    FileOutputFormat.setOutputPath(job, tempDir);
    // System.exit(job.waitForCompletion(true) ? 0 : 1);
   
    if(job.waitForCompletion(true))  
    {  
        //新建一个job
        Job pairJob = new Job(conf, "pair");  
        pairJob.setJarByClass(FindCommonFriends.class); 
        FileInputFormat.addInputPath(pairJob, tempDir); 
        pairJob.setMapperClass(PairMapper.class);  
        pairJob.setReducerClass(PairReducer.class);
        
        FileOutputFormat.setOutputPath(pairJob, outputPath);  

        pairJob.setOutputKeyClass(Text.class);  
        pairJob.setOutputValueClass(Text.class); 

        // 判断output文件夹是否存在，如果存在则删除
        FileSystem fileSystem = outputPath.getFileSystem(conf);
        if (fileSystem.exists(outputPath)) {
          fileSystem.delete(outputPath, true);
        }
        if(pairJob.waitForCompletion(true))
        {
          FileSystem fileSystemTmp = tempDir.getFileSystem(conf);
          fileSystemTmp.delete(tempDir, true);
          System.exit(0);
        }
        
        // System.exit(pairJob.waitForCompletion(true) ? 0 : 1); 
    }
    
  }
}


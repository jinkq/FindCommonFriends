package FindCommonFriends;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Random;
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;

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


public class FindCommonFriends2 {

  public static class FindCommonFriendsMapper
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
        String newKey = new String();
        if(person.compareTo(friend) > 0){
          newKey = friend+","+person;
        }
        else{
          newKey = person+","+friend;
        }
        context.write(new Text(newKey), new Text(friends));
      }
    }
  }

  public static class FindCommonFriendsReducer
       extends Reducer<Text,Text,Text,Text> {
    public static HashSet<String> getFriendsSet(String input) {
      StringTokenizer stringTokenizer = new StringTokenizer(input);
      HashSet<String> friendsSet = new HashSet<>();
      while (stringTokenizer.hasMoreTokens()) {
          friendsSet.add(stringTokenizer.nextToken());
      }
      return friendsSet;
    }

    @Override
    public void reduce(Text key, Iterable<Text> values,
                      Context context) throws IOException, InterruptedException {
      Iterator<Text> iterator = values.iterator();
      String friends1 = iterator.next().toString();
      if(iterator.hasNext()) {
          String friends2 = iterator.next().toString();
          HashSet<String> friendSet1 = getFriendsSet(friends1);
          HashSet<String> friendSet2 = getFriendsSet(friends2);
          HashSet<String> commonFriends = new HashSet<>(friendSet1);
          commonFriends.retainAll(friendSet2);
          context.write(key, new Text(String.join(",",commonFriends)));
      }
      // String str=new String();
      // for(Text val:values)
      // {
      //   str+=val.toString();
      //   str += "----";
      // }
      // context.write(key, new Text(str));
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
    Job job = Job.getInstance(conf, "find common friends 2");
    job.setJarByClass(FindCommonFriends2.class);
    job.setMapperClass(FindCommonFriendsMapper.class);
    job.setReducerClass(FindCommonFriendsReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    List<String> otherArgs = new ArrayList<String>();
    for (int i=0; i < remainingArgs.length; ++i) {
      otherArgs.add(remainingArgs[i]);
    }
    FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs.get(1)));

    Path outputPath = new Path(otherArgs.get(1));
    FileSystem fileSystem = outputPath.getFileSystem(conf);
      if (fileSystem.exists(outputPath)) {
        fileSystem.delete(outputPath, true);
      }

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}


package day04;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public class Friend2 {
    public static class MapTask extends Mapper<LongWritable, Text,Text,Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split("\t");
            String[] friends = splits[1].split(",");
            Arrays.sort(friends);
            for (int i = 0; i < friends.length-1; i++) {
                for (int j = i+1; j < friends.length; j++) {
                    context.write(new Text(friends[i]+"-->"+friends[j]),new Text(splits[0]));
                }
            }

        }
    }
    public static class ReduceTask extends Reducer<Text,Text,Text,Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuffer sb = new StringBuffer();
            boolean flag = true;
            for (Text value : values) {
                if (flag) {
                    sb.append(value);
                    flag = false;
                } else {
                    sb.append(","+value);
                }
            }
            context.write(key,new Text(sb.toString()));
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration entries = new Configuration();
        Job job = Job.getInstance(entries);
        job.setMapperClass(Friend2.MapTask.class);
        job.setReducerClass(Friend2.ReduceTask.class);
        job.setJarByClass(Friend2.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //设置输入输出路径
        //文件存在，删除
        File file = new File("D:\\aaaaa");
        if (file.exists()) {
            FileUtils.deleteDirectory(file);
        }
        FileInputFormat.addInputPath(job,new Path("D:\\friend"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\aaaaa"));
        boolean flag = job.waitForCompletion(true);
        System.out.println(flag?"老铁没毛病":"老铁出bug了");
    }
}

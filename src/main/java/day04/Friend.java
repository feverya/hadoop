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
import java.util.HashMap;
import java.util.Map;

public class Friend {
    public static class MapTask extends Mapper<LongWritable, Text,Text,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(":");
            String user = split[0];
            String[] friends = split[1].split(",");
            for (String friend : friends) {
                context.write(new Text(friend),new Text(user));
            }

        }
    }
    public static class ReduceTask extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuffer sb = new StringBuffer();
            boolean flag = true;
            for (Text value : values) {
                if (flag) {
                    sb.append(value);
                    flag = false;
                } else {
                    sb.append(",").append(value);
                }


            }
            context.write(key,new Text(sb.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration entries = new Configuration();
        Job job = Job.getInstance(entries);
        job.setMapperClass(Friend.MapTask.class);
        job.setReducerClass(Friend.ReduceTask.class);
        job.setJarByClass(Friend.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //设置输入输出路径
        //文件存在，删除
        File file = new File("D:\\friend");
        if (file.exists()) {
            FileUtils.deleteDirectory(file);
        }
        FileInputFormat.addInputPath(job,new Path("D:\\friend.txt"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\friend"));
        boolean flag = job.waitForCompletion(true);
        System.out.println(flag?"老铁没毛病":"老铁出bug了");
    }
}

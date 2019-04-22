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
import org.codehaus.jackson.map.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class Movie {
    public static class MapTask extends Mapper<LongWritable, Text,Text,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            ObjectMapper mapper = new ObjectMapper();
            MovieBean movieBean = new MovieBean();
            movieBean = mapper.readValue(value.toString(), MovieBean.class);
            context.write(new Text(movieBean.getMovie()),new Text(movieBean.getRate()));
        }
    }
    public static class ReduceTask extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<Text> list = new ArrayList<>();
//            double sum = 0;
//            double count = 0;
            for (Text value : values) {
//                count++;
//                sum+=Integer.parseInt(value.toString());
                list.add(value);
            }

            Object[] objects = list.toArray();
            Arrays.sort(objects);
            for (int i = 0; i < objects.length; i++) {
                context.write(key,new Text(objects[i].toString()));
            }
//            String avg = String.valueOf(sum/count);
//            context.write(key,new Text(new String(avg)));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration entries = new Configuration();
        Job job = Job.getInstance(entries);
        job.setMapperClass(Movie.MapTask.class);
        job.setReducerClass(Movie.ReduceTask.class);
        job.setJarByClass(Movie.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //设置输入输出路径
        //文件存在，删除
        File file = new File("D:\\max2");
        if (file.exists()) {
            FileUtils.deleteDirectory(file);
        }
        FileInputFormat.addInputPath(job,new Path("D:\\movie.json"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\max2"));
        boolean flag = job.waitForCompletion(true);
        System.out.println(flag?"老铁没毛病":"老铁出bug了");
    }
}

package com.sazid.mapreduce;

import com.sazid.utils.CompanyInfoWritable;
import com.sazid.utils.Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class Main {
    /**
     *
     * @param args args respectively company.csv, accounts.csv, output
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "company csv to json");

        Path outputPath = new Path(args[2]);
        FileSystem fs = FileSystem.get(new URI(outputPath.toString()), conf);
        //It will delete the output directory if it already exists. don't need to delete it  manually
        fs.delete(outputPath, true);

        job.setJarByClass(Main.class);
//        job.setMapperClass(CompanyCSVMapper.class);
        job.setCombinerClass(CompanyJSONCombiner.class);
        job.setReducerClass(CompanyJSONReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(CompanyInfoWritable.class);
//        job.setInputFormatClass(FileInputFormat.class);
//        FileInputFormat.addInputPath(job, new Path(args[0]));
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CompanyCSVMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, AccountsCSVMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

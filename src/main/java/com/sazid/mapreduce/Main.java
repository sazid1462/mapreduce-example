package com.sazid.mapreduce;

import com.sazid.utils.CompanyInfoWritable;
import com.sazid.utils.FileMerger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

/**
 * Main driver class of hadoop mapreduce job
 */
public class Main {
    // Define some constants to be used across the whole project
    public static final Text accountFieldKey = new Text("accounts");
    public static final Text typeFieldKey = new Text("type");
    static final Text orgNoFieldKey = new Text("orgno");
    private static Configuration conf;
    private static Logger logger = LoggerFactory.getLogger(Main.class);

    /**
     *
     * @param args args respectively company.csv, accounts.csv, output
     * @throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException
     */
    public static void main(String[] args) throws Exception {
        conf = new Configuration();

        Job job = Job.getInstance(conf, "company csv to json");

        Path outputPath = new Path(args[2]);
        FileSystem fs = FileSystem.get(new URI(outputPath.toString()), conf);
        //It will delete the output directory if it already exists. don't need to delete it  manually
        fs.delete(outputPath, true);

        // Configure the job
        job.setJarByClass(Main.class);
        job.setCombinerClass(CompanyJSONCombiner.class);
        job.setReducerClass(CompanyJSONReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(CompanyInfoWritable.class);

        // Register two inputs and mapper classes for company and accounts CSV inputs
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CompanyCSVMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, AccountsCSVMapper.class);
        // Set the output path
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        // Wait for the job to finish.
        job.waitForCompletion(true);

        // Merge the part files to single .json file for each company
        RemoteIterator<LocatedFileStatus> fileList = fs.listFiles(outputPath, true);
        while (fileList.hasNext()) {
            Path inp = fileList.next().getPath().getParent();
            if (!inp.toString().endsWith("__dir")) continue;
            FileMerger.merge(inp.toString(),
                    args[2]+"/"+inp.getName().replace("__dir", ".json"),
                    0, true, true);
        }
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

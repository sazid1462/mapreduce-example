package com.sazid.mapreduce;

import com.google.gson.Gson;
import com.sazid.utils.CompanyInfoWritable;
import com.sazid.utils.Config;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.LinkedHashMap;
import java.util.StringTokenizer;

public class AccountsCSVMapper extends Mapper<LongWritable, Text, Text, CompanyInfoWritable> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        if (key.get() > 0) {
            Gson gson = new Gson();
            Config config = Config.getConfig();

            Reader in = new StringReader(value.toString());
            CompanyInfoWritable accounts = new CompanyInfoWritable();
            accounts.put(new Text("type"), new Text("accounts"));
            Iterable<CSVRecord> records = CSVFormat.RFC4180.parse(in);
            int colID = 0;
            for (CSVRecord record : records) {
                StringTokenizer headers = new StringTokenizer(config.get("accounts.column.headers").toString(), ",");
                while (headers.hasMoreTokens()) {
                    String header = headers.nextToken().trim().toLowerCase();
                    accounts.put(new Text(header), new Text(record.get(colID++)));
                }
                context.write(new Text(record.get(0)), accounts);
            }
        } else {
            System.out.println("Skipping header row with value: " + value.toString());
        }
    }
}

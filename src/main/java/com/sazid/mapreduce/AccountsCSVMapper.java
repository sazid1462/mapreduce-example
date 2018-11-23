package com.sazid.mapreduce;

import com.sazid.utils.CompanyInfoWritable;
import com.sazid.utils.Config;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

import static com.sazid.mapreduce.Main.typeFieldKey;

public class AccountsCSVMapper extends Mapper<LongWritable, Text, Text, CompanyInfoWritable> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        if (key.get() > 0) {
            Config config = Config.getConfig();
            while (config.get("accounts.column.headers") == null) wait(10);
            String[] headers = config.get("accounts.column.headers").toString().split(",");
            Reader in = new StringReader(value.toString());
            CompanyInfoWritable accounts = new CompanyInfoWritable();
            accounts.put(typeFieldKey, new Text("accounts"));
            Iterable<CSVRecord> records = CSVFormat.RFC4180.parse(in);
            for (CSVRecord record : records) {
                CompanyCSVMapper.prepareCompanyInfo(accounts, record, headers);
                context.write(new Text(record.get(1)), accounts);
            }
        } else {
            System.out.println("Collecting header row with value: " + value.toString());
            Config config = Config.getConfig();
            config.put("accounts.column.headers", value.toString());
        }
    }
}

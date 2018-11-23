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

public class CompanyCSVMapper extends Mapper<LongWritable, Text, Text, CompanyInfoWritable> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        if (key.get() > 0) {
            Config config = Config.getConfig();
            while (config.get("company.column.headers") == null) wait(10);
            String[] headers = config.get("company.column.headers").toString().split(",");

            Reader in = new StringReader(value.toString());
            CompanyInfoWritable company = new CompanyInfoWritable();
            company.put(typeFieldKey, new Text("company"));
            Iterable<CSVRecord> records = CSVFormat.RFC4180.parse(in);
            for (CSVRecord record : records) {
                prepareCompanyInfo(company, record, headers);
                context.write(new Text(record.get(0)), company);
            }
        } else {
            System.out.println("Collecting header row with value: " + value.toString());
            Config config = Config.getConfig();
            config.put("company.column.headers", value.toString());
        }
    }

    static void prepareCompanyInfo(CompanyInfoWritable company, CSVRecord record, String[] headers) {
        int colID = 0;
        for (String header : headers) {
            header = header.trim().toLowerCase();
            if (header.equals("")) {
                colID++;
                continue;
            }
            if (header.equals("org_number")) header = "orgno";
            String val = record.get(colID++);
            if (val==null || val.equals("")) continue;
            company.put(new Text(header), new Text(val));
        }
    }
}

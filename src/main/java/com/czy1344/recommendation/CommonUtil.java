package com.czy1344.recommendation;

import org.apache.hadoop.io.Text;

import java.util.Optional;

/**
 * @author czy
 * @since 2021-03-10 18:12
 */
public class CommonUtil {


    public static String aggregationString(Iterable<Text> values) {
        StringBuilder builder = new StringBuilder();

        for (Text value : values) {
            builder.append(value.toString()).append(",");
        }

        String line = null;
        if (builder.toString().endsWith(",")) {
            line = builder.substring(0, builder.length() - 1);
        }
        return Optional.ofNullable(line).orElse("异常数据");
    }
}

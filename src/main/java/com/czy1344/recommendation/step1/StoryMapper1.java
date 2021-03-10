package com.czy1344.recommendation.step1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author czy
 * @since 2021-03-10 15:07
 * 对故事列表 标签进行转置
 *
 * 输入：
 * 故事id 标签_1,标签_1,标签_1
 * --------------------------------------
 *
 * 输出：
 * 标签 故事id_1
 * 标签 故事id_1
 * 标签 故事id_1
 */
public class StoryMapper1 extends Mapper<LongWritable, Text, Text, Text> {
    Text k = new Text();
    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] line = value.toString().split(" ");
        String storyId = line[0];
        String[] tags = line[1].split(",");

        for (String tag : tags) {
            String[] tagScore = tag.split("_");

            String tagId = tagScore[0];
            String score = tagScore[1];

            k.set(tagId);
            v.set(storyId + "_" + score);

            context.write(k, v);
        }
    }
}

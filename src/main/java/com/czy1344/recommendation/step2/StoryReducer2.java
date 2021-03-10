package com.czy1344.recommendation.step2;


import com.czy1344.recommendation.CommonUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Optional;

/**
 * @author czy
 * @since 2021-03-10 15:30
 * 输入：
 * 用户id 标签id_相关度
 * 用户id 标签id_相关度
 * 用户id 标签id_相关度
 *
 * ---------------------------------
 * 输出
 * 用户id 标签id_相关度,标签id_相关度,标签id_相关度
 */
public class StoryReducer2 extends Reducer<Text,Text,Text,Text> {
    Text k = new Text();
    Text v = new Text();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String line = CommonUtil.aggregationString(values);
        k.set(key);
        v.set(line);
        context.write(k,v);
    }
}

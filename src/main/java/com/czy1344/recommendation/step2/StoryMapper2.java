package com.czy1344.recommendation.step2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * @author czy
 * @since 2021-03-10 16:19
 * 输入：
 * 用户 故事-评分,故事-评分,故事-评分
 * ---------------------------------
 * 缓存： step1的输出
 * ---------------------------------
 * 输出：
 * 用户id 标签id_相关度
 * 用户id 标签id_相关度
 * 用户id 标签id_相关度
 */
public class StoryMapper2 extends Mapper<LongWritable, Text, Text, Text> {
    private static String path;
    private Text k = new Text();
    private Text v = new Text();
    private List<String> cacheStep1List = new ArrayList<>();
    private DecimalFormat df = new DecimalFormat("0.00");

    /**
     * 在map执行之前执行，该方法只会执行一次
     * 将所有数据读入缓存中
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        try (FileReader fileReader = new FileReader(path);
             BufferedReader reader = new BufferedReader(fileReader)) {
            // 读入缓存中
            String line = null;
            while ((line = reader.readLine()) != null) {
                cacheStep1List.add(line);
            }
        }


    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split(" ");
        String userId = line[0];
        String[] storyScore = line[1].split(",");


        for (String matrix : cacheStep1List) {
            String[] matrixArr = matrix.split(" ");
            String matrix1 = matrixArr[0];
            String[] matrix2Arr = matrixArr[1].split(",");

            double result = 0;

            for (String item : matrix2Arr) {
                String[] itemArr = item.split("_");
                String storyId = itemArr[0];
                String itemScore = itemArr[1];

                for (String item2 : storyScore) {
                    String[] item2Arr = item2.split("_");
                    String storyId2 = item2Arr[0];
                    String score = item2Arr[1];
                    if (storyId.equals(storyId2)) {
                        result += Double.parseDouble(itemScore) * Double.parseDouble(score);
                    }
                }

            }
            if (result != 0){
                k.set(userId);
                v.set(matrix1 + "_" + df.format(result));
                context.write(k,v);
            }
        }
    }
}

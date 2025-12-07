import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class JoinRequête1 {
    private static final String INPUT_PATH = "inputAnalytique1/";
    private static final String OUTPUT_PATH = "output/joinAnalytique1-";
    private static final Logger LOG = Logger.getLogger(JoinRequête1.class.getName());

    static {
        System.setProperty("java.util.logging.SimpleFormatter.format", "%5$s%n%6$s");

        try {
            FileHandler fh = new FileHandler("out.log");
            fh.setFormatter(new SimpleFormatter());
            LOG.addHandler(fh);
        } catch (SecurityException | IOException e) {
            System.exit(1);
        }
    }

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {


        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            FileSplit split = (FileSplit) context.getInputSplit();
            String fileName = split.getPath().getName();

            String keyId;
            String ecrit;
            String line = value.toString();
            String[] elems = line.split(",");
            if (elems.length<6){return;}

            if ( elems[0].equals("ID_DATE")  || elems[0].equals("ID_CONTENU")){return;}

            if (fileName.equals("contenu_dimension.csv")){
                keyId = elems[0];
                ecrit = fileName.substring(0,2)+"|"+elems[5];
            }
            else{
                keyId   = elems[5];
                ecrit = fileName.substring(0,2)+"|"+elems[elems.length-1];
            }
            context.write(new Text(keyId), new Text(ecrit));
        }
    }


    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {


            String[] elems = new String[10];


            HashMap<String,String> genreById = new HashMap();
            HashMap<String,Integer> streamsValidesById = new HashMap();

            for (Text val : values) {
                elems = val.toString().split("\\|");
                if (elems[0].startsWith("con")) {
                    String genre = elems[1];
                    genreById.put(key.toString(),genre);
                }
                if (elems[0].startsWith("str")) {
                        int temp = streamsValidesById.get(key.toString());
                        if (streamsValidesById.get(key.toString())==null){temp=0;}//LA
                        streamsValidesById.put(key.toString(), Integer.parseInt(elems[1]+temp));
                }
            }

              for (String id :  streamsValidesById.keySet()){
                  if (genreById.get(id)!=null){
                      context.write(new Text(key+","), new Text(genreById.get(id)+","+streamsValidesById.get(id)));
                  }
              }


        }
    }


    public static class Map2 extends Mapper<LongWritable, Text, Text, Text> {


        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            FileSplit split = (FileSplit) context.getInputSplit();
            String fileName = split.getPath().getName();

            String keyId;
            String ecrit;
            String line = value.toString();
            String[] elems = line.split(",");

            if ( elems[0].equals("ID_DATE")  || elems[0].equals("ID_CONTENU")){return;}

            if (fileName.equals("contenu_dimension.csv")){
                keyId = elems[0];
                ecrit = fileName.substring(0,2)+"|"+elems[elems.length-1];
            }
            else{
                keyId   = elems[5];
                ecrit = fileName.substring(0,2)+"|"+elems[5];
            }
            context.write(new Text(keyId), new Text(ecrit));
        }
    }


    public static class Reduce2 extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {


            String[] elems = new String[10];
            String customerName = "";
            ArrayList<String> orderComments = new ArrayList<>();

            for (Text val : values) {
                elems = val.toString().split("\\|");
                if (elems[0].startsWith("c")) {
                    customerName = elems[1];
                }
                if (elems[0].startsWith("o")) {
                    orderComments.add(elems[1]);
                }
            }
            if (!customerName.isEmpty()){

                for (String com : orderComments){
                    context.write(key, new Text(customerName+" | "+com));
                }
            }

        }
    }

    public static void main(String[] args) throws Exception {


        //Premier job : Join entre les deux tables
        Configuration conf = new Configuration();

        Job job = new Job(conf, "Join");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        Path temp = new Path(OUTPUT_PATH+"_TEMP" + Instant.now().getEpochSecond());
        FileOutputFormat.setOutputPath(job,temp);
        job.waitForCompletion(true);

        //Second job : Group by genre
/*
        Configuration conf2 = new Configuration();

        Job job2 = new Job(conf, "groupBy");


        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);


        job2.setMapperClass(Map2.class);
        job2.setReducerClass(Reduce2.class);

        job2.setOutputValueClass(Text.class);

        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);


        FileInputFormat.addInputPath(job2, temp);
        FileOutputFormat.setOutputPath(job2, new Path(OUTPUT_PATH + Instant.now().getEpochSecond()));
        job2.waitForCompletion(true);
*/
    }
}
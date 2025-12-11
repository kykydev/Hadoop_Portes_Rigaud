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
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class JoinRequête4 {
    private static final String INPUT_PATH = "inputAnalytique4/";
    private static final String OUTPUT_PATH = "output/joinAnalytique4-";
    private static final Logger LOG = Logger.getLogger(JoinRequête4.class.getName());

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
            String line = value.toString();

            if (key.get() == 0 && (line.startsWith("ID_segment") || line.startsWith("ID_date"))) {
                return;
            }

            String[] elems = line.split(",");
            if (elems.length < 4) { return; }

            String keyId = "";
            String ecrit = "";

            if (fileName.equals("segment_utilisateurs_dimension.csv")) {
                // [ID_segment(0), nom_tranche_d_age(1), ...]
                keyId = elems[0];
                ecrit = "seg|" + elems[1].trim();
            }
            // CORRECTION DU NOM DU FICHIER : "abonnement_facts.csv"
            else if (fileName.equals("abonnement_facts.csv")) {
                // [ID_date(0), ID_offre(1), ID_region(2), ID_segment(3), ...]
                keyId = elems[3];
                ecrit = "abo|1";
            } else {
                return;
            }

            if (!keyId.isEmpty()) {
                context.write(new Text(keyId), new Text(ecrit));
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String ageSegment = null;
            int totalAboCount = 0;

            for (Text val : values) {
                String[] elems = val.toString().split("\\|");

                if (elems.length < 2) continue;

                if (elems[0].startsWith("seg")) {
                    ageSegment = elems[1].trim();
                }
                if (elems[0].startsWith("abo")) {
                    try {
                        totalAboCount += Integer.parseInt(elems[1].trim());
                    } catch (NumberFormatException e) {

                    }
                }
            }

            if (ageSegment != null && totalAboCount > 0) {
                // Sortie du Job 1 : Clé = Tranche d'âge, Valeur = Compte Total
                context.write(new Text(ageSegment), new Text(String.valueOf(totalAboCount)));
            }
        }
    }


    public static class Map2 extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            String[] elems = line.split("\\t");

            if (elems.length < 2) { return; }

            String keyId = elems[0].trim();
            String ecrit = elems[1].trim();

            context.write(new Text(keyId), new Text(ecrit));
        }
    }

    public static class Reduce2 extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;

            for (Text val : values) {
                try {
                    sum += Integer.parseInt(val.toString().trim());
                } catch (NumberFormatException e) {

                }
            }
            // Sortie finale : Tranche d'âge (Clé), Somme Totale (Valeur)
            context.write(new Text(key+","), new Text(Integer.toString(sum)));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Path temp = new Path(OUTPUT_PATH + "_TEMP-" + Instant.now().getEpochSecond());

        // Job 1 : Jointure et première agrégation
        Job job1 = new Job(conf, "Join-Dimension-Fact");

        job1.setJarByClass(JoinRequête4.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        job1.setMapperClass(Map.class);
        job1.setReducerClass(Reduce.class);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job1, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job1, temp);

        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        // Job 2 : Group by Tranche d'âge
        Configuration conf2 = new Configuration();

        Job job2 = new Job(conf2, "GroupBy-Age-Segment");

        job2.setJarByClass(JoinRequête4.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.setMapperClass(Map2.class);
        job2.setReducerClass(Reduce2.class);

        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job2, temp);
        FileOutputFormat.setOutputPath(job2, new Path(OUTPUT_PATH + Instant.now().getEpochSecond()));

        job2.waitForCompletion(true);
        temp.getFileSystem(conf).delete(temp, true);
    }
}
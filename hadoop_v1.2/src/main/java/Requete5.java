import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
import java.util.List;

public class Requete5 {

    private static final String INPUT_PATH = "inputAnalytique5/";
    private static final String TEMP_PATH_1 = "output/temp_req4_step1_";
    private static final String TEMP_PATH_2 = "output/temp_req4_step2_";
    private static final String OUTPUT_PATH = "output/resultat_final_req4_";

    //jointure abo_fact + offre
    public static class JoinMapper1 extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] elems = line.split(",", -1);

            if (elems.length < 2) return;

            FileSplit split = (FileSplit) context.getInputSplit();
            String fileName = split.getPath().getName();
            String idOffre = "";
            String information = "";

            try {
                // fichier abonnement_facts.csv
                if (fileName.equals("abonnement_facts.csv")) {
                    if (elems[1].replace("\"", "").equals("ID_offre")) return; // Skip Header

                    idOffre = elems[1].replace("\"", "").trim(); // Clé de jointure
                    String idRegion = elems[2].replace("\"", "").trim(); // Pour la suite

                    String desaboStr = "0";
                    //index 6, desabonnements
                    if (elems.length > 6) {
                        String cleanDesabo = elems[6].replace("\"", "").trim();
                        if (!cleanDesabo.isEmpty()) {
                            desaboStr = cleanDesabo;
                        }
                    }

                    information = "FACT:" + idRegion + "|" + desaboStr;
                }

                //fichier offre_dimension.csv
                else if (fileName.equals("offre_dimension.csv")) {
                    if (elems[0].replace("\"", "").equals("ID_offre")) return;

                    idOffre = elems[0].replace("\"", "").trim(); // Clé de jointure
                    String typeOffre = elems[2].replace("\"", "").trim();

                    information = "DIM:" + typeOffre;
                }

                if (!idOffre.isEmpty() && !information.isEmpty()) {
                    context.write(new Text(idOffre), new Text(information));
                }

            } catch (Exception e) {
                //
            }
        }
    }

    public static class JoinReducer1 extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String typeOffre = "Inconnu";
            List<String> facts = new ArrayList<>();

            for (Text val : values) {
                String info = val.toString();
                if (info.startsWith("DIM:")) {
                    typeOffre = info.substring(4);
                } else if (info.startsWith("FACT:")) {
                    facts.add(info.substring(5));
                }
            }

            if (!typeOffre.equals("Inconnu")) {
                for (String f : facts) {
                    String[] parts = f.split("\\|");
                    if (parts.length == 2) {
                        String idRegion = parts[0];
                        String desabo = parts[1];

                        context.write(new Text(idRegion), new Text(typeOffre + "|" + desabo));
                    }
                }
            }
        }
    }

    //jointure result j1 + regions
    public static class JoinMapper2 extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit split = (FileSplit) context.getInputSplit();
            String fileName = split.getPath().getName();
            String idRegion = "";
            String information = "";

            try {
                if (fileName.startsWith("part-r-")) {
                    String line = value.toString();
                    String[] parts = line.split("\t");
                    if (parts.length == 2) {
                        idRegion = parts[0];
                        information = "FACT:" + parts[1];
                    }
                }
                //fichier region_dimension.csv
                else if (fileName.equals("region_dimension.csv")) {
                    String line = value.toString();
                    String[] elems = line.split(",", -1);

                    if (elems.length < 7) return;
                    if (elems[0].replace("\"", "").equals("ID_region")) return;

                    idRegion = elems[0].replace("\"", "").trim();
                    String continent = elems[6].replace("\"", "").trim();

                    information = "DIM:" + continent;
                }

                if (!idRegion.isEmpty() && !information.isEmpty()) {
                    context.write(new Text(idRegion), new Text(information));
                }

            } catch (Exception e) {
                //
            }
        }
    }

    public static class JoinReducer2 extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String continent = "Inconnu";
            List<String> facts = new ArrayList<>();

            for (Text val : values) {
                String info = val.toString();
                if (info.startsWith("DIM:")) {
                    continent = info.substring(4);
                } else if (info.startsWith("FACT:")) {
                    facts.add(info.substring(5));
                }
            }

            if (!continent.equals("Inconnu")) {
                for (String f : facts) {
                    String[] parts = f.split("\\|");
                    if (parts.length == 2) {
                        String typeOffre = parts[0];
                        String desabo = parts[1];

                        context.write(new Text(typeOffre + " - " + continent), new Text(desabo));
                    }
                }
            }
        }
    }

    //somme desabo
    public static class GroupByMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split("\t");

            if (parts.length == 2) {
                String groupe = parts[0];
                try {
                    String nbStr = parts[1].replace(",", ".");
                    double nbDouble = Double.parseDouble(nbStr);
                    context.write(new Text(groupe), new LongWritable((long)nbDouble));
                } catch (NumberFormatException e) {
                    //
                }
            }
        }
    }

    public static class GroupByReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long somme = 0;

            for (LongWritable val : values) {
                somme += val.get();
            }

            context.write(key, new LongWritable(somme));
        }
    }

    //main
    public static void main(String[] args) throws Exception {

        String timestamp = String.valueOf(Instant.now().getEpochSecond());
        String tempDir1 = TEMP_PATH_1 + timestamp;
        String tempDir2 = TEMP_PATH_2 + timestamp;
        String finalDir = OUTPUT_PATH + timestamp;

        //1
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Job 1: Join Fact-Offre");
        job1.setJarByClass(Requete5.class);
        job1.setMapperClass(JoinMapper1.class);
        job1.setReducerClass(JoinReducer1.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job1, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job1, new Path(tempDir1));

        if (!job1.waitForCompletion(true)) System.exit(1);

        //2
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Job 2: Join Result-Region");
        job2.setJarByClass(Requete5.class);
        job2.setMapperClass(JoinMapper2.class);
        job2.setReducerClass(JoinReducer2.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job2, new Path(tempDir1)); // Sortie Job 1
        FileInputFormat.addInputPath(job2, new Path(INPUT_PATH)); // Lit aussi Region Dim
        FileOutputFormat.setOutputPath(job2, new Path(tempDir2));

        if (!job2.waitForCompletion(true)) System.exit(1);

        //3
        Configuration conf3 = new Configuration();
        Job job3 = Job.getInstance(conf3, "Job 3: Somme Desabonnements");
        job3.setJarByClass(Requete5.class);
        job3.setMapperClass(GroupByMapper.class);
        job3.setReducerClass(GroupByReducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(LongWritable.class); // On sort un Long (entier)
        job3.setInputFormatClass(TextInputFormat.class);
        job3.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job3, new Path(tempDir2));
        FileOutputFormat.setOutputPath(job3, new Path(finalDir));

        System.exit(job3.waitForCompletion(true) ? 0 : 1);
    }
}
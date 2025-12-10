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

public class Requete2 {

    private static final String INPUT_PATH = "inputAnalytique2/";
    private static final String TEMP_PATH = "output/temp_join_pays_age_";
    private static final String OUTPUT_PATH = "output/resultat_final_pays_age_";

    //jointure
    public static class JoinMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] elems = line.split(",", -1); //pour garder string vides car csv bizarre

            if (elems.length < 2) return;

            FileSplit split = (FileSplit) context.getInputSplit();
            String fileName = split.getPath().getName();

            String idUtilisateur = "";
            String information = "";

            try {
                //fichier 1, stream_fact.csv, id index 2, revenu index 9
                if (fileName.equals("streams_fact.csv")) {
                    if (elems.length < 3) return;

                    if (elems[2].replace("\"", "").equals("ID_UTILISATEUR")) return;

                    //index 2 ID_UTILISATEUR
                    idUtilisateur = elems[2].replace("\"", "").trim();

                    //index 9 REVENU_GENERE_USD
                    String revenuStr = "0";
                    if (elems.length > 9) {
                        String cleanRev = elems[9].replace("\"", "").trim();
                        if (!cleanRev.isEmpty()) {
                            revenuStr = cleanRev;
                        }
                    }
                    information = "REVENU:" + revenuStr;
                }

                //fichier 2, utilisateur_statique_dimension.csv, pays index 2, id index 0
                else if (fileName.equals("utilisateur_statique_dimension.csv")) {
                    if (elems.length < 3) return;

                    if (elems[0].replace("\"", "").equals("ID_UTILISATEUR")) return; // Skip header

                    idUtilisateur = elems[0].replace("\"", "").trim();
                    //index 2 PAYS_INSCRIPTION
                    String pays = elems[2].replace("\"", "").trim();
                    information = "PAYS:" + pays;
                }

                //fichier 3, utilisateur_dynamique_dimension.csv, age index 3, id index 0
                else if (fileName.equals("utilisateur_dynamique_dimension.csv")) {
                    if (elems.length < 4) return;

                    if (elems[0].replace("\"", "").equals("ID_UTILISATEUR")) return; // Skip header

                    idUtilisateur = elems[0].replace("\"", "").trim();
                    //index 3 TRANCHE_AGE_CALCULEE
                    String age = elems[3].replace("\"", "").trim();
                    information = "AGE:" + age;
                }

                if (!idUtilisateur.isEmpty() && !information.isEmpty()) {
                    context.write(new Text(idUtilisateur), new Text(information));
                }

            } catch (Exception e) {
                //
            }
        }
    }

    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String pays = "Inconnu";
            String age = "Inconnu";
            List<String> listeRevenus = new ArrayList<>();

            for (Text val : values) {
                String info = val.toString();

                if (info.startsWith("PAYS:")) {
                    pays = info.substring(5);
                } else if (info.startsWith("AGE:")) {
                    age = info.substring(4);
                } else if (info.startsWith("REVENU:")) {
                    listeRevenus.add(info.substring(7));
                }
            }

            if (!pays.equals("Inconnu") && !age.equals("Inconnu")) {
                for (String rev : listeRevenus) {
                    context.write(new Text(pays + " - " + age), new Text(rev));
                }
            }
        }
    }


    //group by, calcule moyenne
    public static class GroupByMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split("\t");

            if (parts.length == 2) {
                String groupe = parts[0];
                try {
                    String revStr = parts[1].replace(",", ".");
                    double revenu = Double.parseDouble(revStr);
                    context.write(new Text(groupe), new DoubleWritable(revenu));
                } catch (NumberFormatException e) {
                    // Ignorer
                }
            }
        }
    }

    public static class GroupByReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double somme = 0;
            int compte = 0;

            for (DoubleWritable val : values) {
                somme += val.get();
                compte++;
            }

            if (compte > 0) {
                double moyenne = somme / compte;
                context.write(key, new DoubleWritable(moyenne));
            }
        }
    }

    //main
    public static void main(String[] args) throws Exception {

        String tempDir = TEMP_PATH + Instant.now().getEpochSecond();
        String finalDir = OUTPUT_PATH + Instant.now().getEpochSecond();

        // 1
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Job 1: Jointure");
        job1.setJarByClass(Requete2.class);

        job1.setMapperClass(JoinMapper.class);
        job1.setReducerClass(JoinReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job1, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job1, new Path(tempDir));

        if (!job1.waitForCompletion(true)) System.exit(1);

        // 2
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Job 2: Moyenne");
        job2.setJarByClass(Requete2.class);

        job2.setMapperClass(GroupByMapper.class);
        job2.setReducerClass(GroupByReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(DoubleWritable.class);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job2, new Path(tempDir));
        FileOutputFormat.setOutputPath(job2, new Path(finalDir));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
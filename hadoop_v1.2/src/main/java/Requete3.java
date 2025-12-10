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

public class Requete3 {

    private static final String INPUT_PATH = "inputAnalytique2/";
    private static final String TEMP_PATH_1 = "output/temp_req3_step1_";
    private static final String TEMP_PATH_2 = "output/temp_req3_step2_";
    private static final String OUTPUT_PATH = "output/resultat_final_req3_";

    //jointure streams + contenu
    public static class J1Mapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] elems = line.split(",", -1);

            if (elems.length < 2) return;

            FileSplit split = (FileSplit) context.getInputSplit();
            String fileName = split.getPath().getName();
            String idContenu = "";
            String information = "";

            try {
                // fichier streams_fact.csv, ID_CONTENU index 5, ID_USER index 2, DUREE index 7
                if (fileName.equals("streams_fact.csv")) {
                    if (elems.length < 8) return;
                    if (elems[5].replace("\"", "").equals("ID_CONTENU")) return;

                    idContenu = elems[5].replace("\"", "").trim();
                    String idUser = elems[2].replace("\"", "").trim();

                    String dureeStr = "0";
                    String cleanDuree = elems[7].replace("\"", "").trim();
                    if (!cleanDuree.isEmpty()) {
                        dureeStr = cleanDuree;
                    }

                    information = "STREAM:" + idUser + "|" + dureeStr;
                }
                //fichier contenu_dimension.csv, ID_CONTENU index 0, GENRE index 5
                else if (fileName.equals("contenu_dimension.csv")) {
                    if (elems.length < 6) return;
                    if (elems[0].replace("\"", "").equals("ID_CONTENU")) return;

                    idContenu = elems[0].replace("\"", "").trim();
                    String genre = elems[5].replace("\"", "").trim();

                    information = "GENRE:" + genre;
                }

                if (!idContenu.isEmpty() && !information.isEmpty()) {
                    context.write(new Text(idContenu), new Text(information));
                }

            } catch (Exception e) {
                //
            }
        }
    }

    public static class J1Reducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String genre = "Inconnu";
            List<String> streams = new ArrayList<>();

            for (Text val : values) {
                String info = val.toString();
                if (info.startsWith("GENRE:")) {
                    genre = info.substring(6);
                } else if (info.startsWith("STREAM:")) {
                    streams.add(info.substring(7)); // Stocke "ID_USER|DUREE"
                }
            }

            if (!genre.equals("Inconnu")) {
                for (String st : streams) {
                    String[] parts = st.split("\\|");
                    if (parts.length == 2) {
                        String idUser = parts[0];
                        String duree = parts[1];
                        // la sortie c'est ça Clé=ID_USER, Val=GENRE|DUREE
                        context.write(new Text(idUser), new Text(genre + "|" + duree));
                    }
                }
            }
        }
    }

    //jointure resultat j1 et utilisateurs stat+dyn
    public static class J2Mapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit split = (FileSplit) context.getInputSplit();
            String fileName = split.getPath().getName();
            String idUtilisateur = "";
            String information = "";

            try {
                if (fileName.startsWith("part-r-")) {
                    String line = value.toString();
                    String[] parts = line.split("\t");
                    if (parts.length == 2) {
                        idUtilisateur = parts[0];
                        information = "INFO:" + parts[1]; // GENRE|DUREE
                    }
                }
                else {
                    String line = value.toString();
                    String[] elems = line.split(",", -1);
                    if (elems.length < 2) return;

                    // utilisateur_statique, Pays index 2
                    if (fileName.equals("utilisateur_statique_dimension.csv")) {
                        if (elems.length < 3) return;
                        if (elems[0].replace("\"", "").equals("ID_UTILISATEUR")) return;

                        idUtilisateur = elems[0].replace("\"", "").trim();
                        String pays = elems[2].replace("\"", "").trim();
                        information = "PAYS:" + pays;
                    }
                    // utilisateur_dynamique, Age index 3
                    else if (fileName.equals("utilisateur_dynamique_dimension.csv")) {
                        if (elems.length < 4) return;
                        if (elems[0].replace("\"", "").equals("ID_UTILISATEUR")) return;

                        idUtilisateur = elems[0].replace("\"", "").trim();
                        String age = elems[3].replace("\"", "").trim();
                        information = "AGE:" + age;
                    }
                }

                if (!idUtilisateur.isEmpty() && !information.isEmpty()) {
                    context.write(new Text(idUtilisateur), new Text(information));
                }

            } catch (Exception e) {
                //
            }
        }
    }

    public static class J2Reducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String pays = "Inconnu";
            String age = "Inconnu";
            List<String> infosStreams = new ArrayList<>();

            for (Text val : values) {
                String info = val.toString();
                if (info.startsWith("PAYS:")) {
                    pays = info.substring(5);
                } else if (info.startsWith("AGE:")) {
                    age = info.substring(4);
                } else if (info.startsWith("INFO:")) {
                    infosStreams.add(info.substring(5)); // Contient "GENRE|DUREE"
                }
            }

            if (!pays.equals("Inconnu") && !age.equals("Inconnu")) {
                for (String is : infosStreams) {
                    String[] parts = is.split("\\|");
                    if (parts.length == 2) {
                        String genre = parts[0];
                        String duree = parts[1];
                        context.write(new Text(pays + " - " + age + " - " + genre), new Text(duree));
                    }
                }
            }
        }
    }

    //moyenne
    public static class J3Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split("\t");

            if (parts.length == 2) {
                String groupe = parts[0];
                try {
                    String dureeStr = parts[1].replace(",", ".");
                    double duree = Double.parseDouble(dureeStr);
                    context.write(new Text(groupe), new DoubleWritable(duree));
                } catch (NumberFormatException e) {
                    //
                }
            }
        }
    }

    public static class J3Reducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
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

        String timestamp = String.valueOf(Instant.now().getEpochSecond());
        String tempDir1 = TEMP_PATH_1 + timestamp;
        String tempDir2 = TEMP_PATH_2 + timestamp;
        String finalDir = OUTPUT_PATH + timestamp;

        System.out.println("=== DÉBUT REQUETE 3 ===");

        //1
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Job 1: Join Stream-Contenu");
        job1.setJarByClass(Requete3.class);
        job1.setMapperClass(J1Mapper.class);
        job1.setReducerClass(J1Reducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job1, new Path(INPUT_PATH)); // Lit Streams et Contenu
        FileOutputFormat.setOutputPath(job1, new Path(tempDir1));

        if (!job1.waitForCompletion(true)) System.exit(1);

        //2
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Job 2: Join User-ResultatJ1");
        job2.setJarByClass(Requete3.class);
        job2.setMapperClass(J2Mapper.class);
        job2.setReducerClass(J2Reducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job2, new Path(tempDir1)); // Sortie Job 1
        FileInputFormat.addInputPath(job2, new Path(INPUT_PATH)); // Lit aussi Statique et Dynamique
        FileOutputFormat.setOutputPath(job2, new Path(tempDir2));

        if (!job2.waitForCompletion(true)) System.exit(1);

        //3
        Configuration conf3 = new Configuration();
        Job job3 = Job.getInstance(conf3, "Job 3: Moyenne");
        job3.setJarByClass(Requete3.class);
        job3.setMapperClass(J3Mapper.class);
        job3.setReducerClass(J3Reducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(DoubleWritable.class);
        job3.setInputFormatClass(TextInputFormat.class);
        job3.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job3, new Path(tempDir2));
        FileOutputFormat.setOutputPath(job3, new Path(finalDir));

        System.out.println("Lancement Job 3 (Moyenne)...");
        System.exit(job3.waitForCompletion(true) ? 0 : 1);
    }
}
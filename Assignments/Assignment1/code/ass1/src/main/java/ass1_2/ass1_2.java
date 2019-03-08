package ass1_2;

import java.io.*;
import java.net.URI;
import java.util.Date;
import java.util.List;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import twitter4j.*;

public class ass1_2 {

    public static void main(String[] args) throws TwitterException, IOException {
        GetConfiguration conf = new GetConfiguration();
        Twitter twitter = conf.getNewInstance();

        // check the search key word
        if (args.length != 1) {
            System.out.println("You must input exactly ONE input");
            System.exit(-1);
        }

        Paging page = new Paging();
        // create the output folder
        String outputPath =  args[0]+"-search";
        boolean success = (new File(outputPath)).mkdirs();

        String[] timelines = {
                "2019-2-1",
                "2019-2-2",
                "2019-2-3",
                "2019-2-4",
                "2019-2-5",
                "2019-2-6",
        };

        /*
        This for local
         */
//        for(String timeline:timelines){
//            FileWriter file = new FileWriter(outputPath+"/"+timeline+".txt");
//            try{
//                Query query = new Query(args[0]);
//                query.setSince(timeline);
//                query.setUntil(timeline.substring(0,timeline.length()-1)+(char)(timeline.charAt(timeline.length()-1)+1));
//                query.setCount(500);
//                QueryResult result;
//                List<Status> tweets;
//                result = twitter.search(query);
//                tweets = result.getTweets();
//                // now generating output
//                for (Status tweet : tweets) {
//                    String tweetJson = TwitterObjectFactory.getRawJSON(tweet);
////                    System.out.println("+++"+tweetJson);
//                    // write to files
//                    file.write(tweetJson);
//                    file.write("\n");
//                }
//            } catch (IOException e){
//                e.printStackTrace();
//            } finally {
//                file.flush();
//                file.close();
//            }
//        }

        /*
        Now let's go for hdfs version
         */
        InputStream in = null;
        OutputStream out = null;

        try{
            Configuration confHadoop = new Configuration();
            FileSystem fs = FileSystem.get(URI.create(outputPath), confHadoop);
            // for different timelines
            for(String timeline:timelines) {
                String dst = outputPath + "/" + timeline + ".txt";
                Path dstPath = new Path(dst);

                if (fs.exists(dstPath)) {
                    fs.delete(dstPath, true);
                }
                out = fs.create(dstPath, new Progressable() {
                    public void progress() {
                        System.out.println(".");
                    }
                });
                BufferedWriter wr = new BufferedWriter(new OutputStreamWriter(out, "UTF-8"));

                Query query = new Query(args[0]);
                query.setSince(timeline);
                query.setCount(500);
                query.setUntil(timeline.substring(0, timeline.length() - 1) + (char) (timeline.charAt(timeline.length() - 1) + 1));
                QueryResult result;
                List<Status> tweets;
                result = twitter.search(query);
                tweets = result.getTweets();

                // now generating output
                for (Status tweet : tweets) {
                    String tweetJson = TwitterObjectFactory.getRawJSON(tweet);
//                    System.out.println("+++"+tweetJson);
                    // write to files
                    wr.write(tweetJson);
                    wr.write("\n");
//                FileWriter file = new FileWriter(outputPath+"/"+ timeline + ".txt");
                }
            }
        }  catch (IOException e){
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(in);
            IOUtils.closeStream(out);
        }
    }
}

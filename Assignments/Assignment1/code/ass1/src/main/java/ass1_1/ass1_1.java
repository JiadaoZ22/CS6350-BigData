package ass1_1;

import java.io.BufferedInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.util.Progressable;

import javax.print.DocFlavor;

public class ass1_1 {
    public static void main(String[] args){
        String[] books = {
                "http://www.utdallas.edu/~axn112530/cs6350/lab2/input/20417.txt.bz2",
                "http://www.utdallas.edu/~axn112530/cs6350/lab2/input/5000-8.txt.bz2",
                "http://www.utdallas.edu/~axn112530/cs6350/lab2/input/132.txt.bz2",
                "http://www.utdallas.edu/~axn112530/cs6350/lab2/input/1661-8.txt.bz2",
                "http://www.utdallas.edu/~axn112530/cs6350/lab2/input/972.txt.bz2",
                "http://www.utdallas.edu/~axn112530/cs6350/lab2/input/19699.txt.bz2"
        };

        String dirpath = args[0];

        for (String book:books){
            InputStream in = null;
            OutputStream out = null;

            // write to hdfs
            try{
                URL url = new URL(book);
                in = new BufferedInputStream(url.openStream());
                Configuration conf = new Configuration();
                // instead of new URI() in case of syntax error
                FileSystem fs = FileSystem.get(URI.create(dirpath), conf);
                String dst = dirpath + "/" + FilenameUtils.getName(url.getPath());
                System.out.println("dst"+dst);

                Path dstPath = new Path(dst);

                out = fs.create(dstPath, new Progressable() {
                    public void progress() {
                        System.out.println(".");
                    }
                });
                 // file copy, close the stream
                IOUtils.copyBytes(in,out,4096,true);

                // decompress file and delete initial file
                // create the compressor object
                CompressionCodecFactory factory = new CompressionCodecFactory(conf);
                // get the format of compressed file
                CompressionCodec codec = factory.getCodec(dstPath);
                // if the format doesn't exist, exit!
                if (codec == null){
                    System.err.println("No codec found for" + book);
                    System.exit(1);
                }
                // get rid of the file's extension name, outputUri is the output path
                String outputUri = CompressionCodecFactory.removeSuffix(dst, codec.getDefaultExtension());
                // create IN/OUT stream
                in = codec.createInputStream(fs.open((dstPath)));
                out = fs.create(new Path(outputUri));
                // decompress
                IOUtils.copyBytes(in, out, conf,false);

                fs.delete(dstPath, true);
            } catch (Exception e){
                System.out.println(e.getMessage());
                e.printStackTrace();
            } finally {
                IOUtils.closeStream(in);
                IOUtils.closeStream(out);
            }
        }
    }
}

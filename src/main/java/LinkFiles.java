import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

public class LinkFiles {

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Invalid Arguments... <src> [dest]");
            System.exit(0);
        }

        String inputLocation = args[0];
        String outPath = inputLocation + "/merged.out";

        if (args.length == 2) {
            outPath = args[1];
        }

        Configuration configuration = new Configuration();
        try {
            if((FileSystem.get(configuration)).isDirectory(new Path(inputLocation))) {
                System.out.println("Connecting to "+configuration.get("fs.defaultFS"));
                System.out.println("Reading files from " + inputLocation);
                FileSystem fs = FileSystem.get(URI.create(outPath), configuration);
                FileSystem inFs = FileSystem.get(URI.create(inputLocation), configuration);
                OutputStream out = fs.create(new Path(outPath));
                RemoteIterator<LocatedFileStatus> list = inFs.listFiles(new Path(inputLocation), false);
                byte[] buf = new byte[100];
                while(list.hasNext()) {	// Loop through the directory
                    InputStream in = inFs.open(list.next().getPath());
                    int b;
                    while ( (b = in.read(buf)) >= 0) {
                        out.write(buf, 0, b);
                        out.flush();
                    }
                }
                out.close();
                System.out.println("Created output file " + outPath);
            } else {
                System.out.println("Not a directory...");
            }
        } catch (IllegalArgumentException | IOException e1) {
            e1.printStackTrace();
        }
    }
}



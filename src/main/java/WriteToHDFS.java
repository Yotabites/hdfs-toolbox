import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class WriteToHDFS {
	
	private boolean forceOption;
	private boolean deleteOption;
	private short repl;

	private void CopyFiles(File source, String destination, Configuration configuration) {
		if(!source.isDirectory()) {   // if it is a file, copy it.
			InputStream in;
			try {
				Path destPath = new Path(destination);
				in = new BufferedInputStream(new FileInputStream(source));
				FileSystem fs = FileSystem.get(URI.create(destination), configuration);
				fs.setReplication(destPath, repl);

				if (forceOption && fs.exists(destPath)) {    // Delete old files and overwrite
					fs.delete(destPath, false);
				}

				if( !forceOption && fs.exists(destPath)) {	// No force option AND duplicate file
					System.out.println(destination + " exists. Skipping this file. Use -f to force it");
					return;
				}
				OutputStream out = fs.create(destPath, repl);
				IOUtils.copyBytes(in, out, 4096, true);
				System.out.println(destination);

				if(deleteOption)	{
					// Input file is deleted immediately
					if(source.delete()) System.out.println(source.getName() + " deleted");
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		} else { 					// if it is a directory
			FileSystem dfs;
			try {
				dfs = FileSystem.get(configuration);
				dfs.mkdirs(new Path(destination));	// create the directory in HDFS
				
				String[] sourceList = source.list();
				if (sourceList != null) {
					for (String aSourceList : sourceList) {    // Loop through the directory
                        CopyFiles(new File(source, aSourceList), (destination + "/" + aSourceList), configuration);
                    }
                    source.delete();	// deletes the directory
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		
		Configuration configuration = new Configuration();
		System.out.println("Connecting to "+configuration.get("fs.defaultFS"));
		
		WriteToHDFS util = new WriteToHDFS();
		String srcLocation = "";
		String dstLocation = "";

		util.repl = 3;	// Default replication value

		try {
			for (String arg : args) {
				if (arg.startsWith("-repl"))
					util.repl = Short.parseShort(arg.substring(arg.indexOf("=") + 1));
				if ("-f".equals(arg.trim()))
					util.forceOption = true;
				if ("-d".equals(arg.trim()))
					util.deleteOption = true;
			}
		} catch (NumberFormatException n) {
			System.out.println("Invalid replication factor. Example: -repl=2");
			System.out.println("Invalid Arguments... [-f | -d | -repl=n] <src> <dest>");
			System.exit(0);
		}

		if(args.length >= 2) {
			srcLocation = args[args.length - 2];
			dstLocation = args[args.length - 1];
		} else {
			System.out.println("Invalid Arguments... [-f | -d | -repl=n] <src> <dest>");
			System.exit(0);
		}

		// Calling the recursive method (if it is a directory)
		util.CopyFiles(new File(srcLocation), dstLocation, configuration);
		
	}
}	

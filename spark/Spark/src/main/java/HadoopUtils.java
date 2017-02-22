import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import java.io.IOException;

// http://www.markhneedham.com/blog/2014/11/30/spark-write-to-csv-file-with-header-using-saveasfile/
public class HadoopUtils {
    public static boolean copyMergeWithHeader(FileSystem srcFS, Path srcDir, FileSystem dstFS, Path dstFile, boolean deleteSource, Configuration conf, String header) throws IOException {
        dstFile = checkDest(srcDir.getName(), dstFS, dstFile, false);
        if (!srcFS.getFileStatus(srcDir).isDir())
            return false;

        FSDataOutputStream out = dstFS.create(dstFile);
        try {
            if (header != null) {
                System.out.println("Writing csv header");
                out.write((header + "\n").getBytes("UTF-8"));
            }

            FileStatus[] contents = srcFS.listStatus(srcDir);
            for (int i = 0; i < contents.length; ++i) {
                if (!contents[i].isDir()) {
                    FSDataInputStream in = srcFS.open(contents[i].getPath());
                    try {
                        IOUtils.copyBytes(in, out, conf, false);
                    } finally {
                        in.close();
                    }
                }
            }
        } finally {
            out.close();
        }

        return deleteSource ? srcFS.delete(srcDir, true) : true;
    }

    private static Path checkDest(String srcName, FileSystem dstFS, Path dst, boolean overwrite) throws IOException {
        if (dstFS.exists(dst)) {
            FileStatus sdst = dstFS.getFileStatus(dst);
            if (sdst.isDir()) {
                if (null == srcName) {
                    throw new IOException("Target " + dst + " is a directory");
                }

                return checkDest((String) null, dstFS, new Path(dst, srcName), overwrite);
            }

            if (!overwrite) {
                throw new IOException("Target " + dst + " already exists");
            }
        }
        return dst;
    }
}
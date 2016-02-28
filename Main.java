import org.opencv.core.Core;


import java.io.File;


public class Main {


   public static void main(String... args) {


       System.loadLibrary(Core.NATIVE_LIBRARY_NAME);


if (args.length == 0) {
   System.err.println("Usage Main /path/to/images");
   System.exit(1);
}


       File[] files = new File(args[0]).listFiles();
       showFiles(files);
   }


   public static void showFiles(File[] files) {
       DetectFaces faces = new DetectFaces();
       for (File file : files) {
           if (file.isDirectory()) {
               System.out.println("Directory: " + file.getName());
               showFiles(file.listFiles()); // Calls same method again.
           } else {
               System.out.println("File: " + file.getAbsolutePath());
               faces.run(file.getAbsolutePath());
           }
       }
   }
}


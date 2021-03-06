# verdant-octo-woof
Spring-boot+SwaggerUI RESTful for Image processing using OpenCV on Hadoop using HIPI.

We can create a Hadoop MapReduce Job with Spring Data Apache Hadoop by following these steps:

##0.Homebrew install hadoop

http://ericlondon.com/2014/08/01/hadoop-pig-ruby-map-reduce-on-osx-via-homebrew.html

http://amodernstory.com/2014/09/23/installing-hadoop-on-mac-osx-yosemite/

##1.Homebrew install open-cv with java

brew tap homebrew/science

brew install opencv --with-java

1.Get the required dependencies by using Maven.

##2.Create the mapper component.

##3.Create the reducer component.

##4.Configure the application context.

##5.Load the application context when the application starts.

##6.SwaggerUI RESTful controllers.

##7.Image processing using OpenCV on Hadoop: 

###Mapper:

    1.Load OpenCV native library
    2.Create CascadeClassifier
    3.onvert HIPI FloatImage to OpenCV Mat
    4.Detect and count faces in the image
    5.Write number of faces detected to context



###Reducer:

    1.Count number of files processed
    2.Count number of faces detected
    3.Output number of files and faces detected



http://dinesh-malav.blogspot.com/2015/05/image-processing-using-opencv-on-hadoop.html

https://github.com/GopiKrishnan-V/hipi-hadoop

# References

http://amodernstory.com/2014/09/23/installing-hadoop-on-mac-osx-yosemite/

http://www.petrikainulainen.net/programming/apache-hadoop/creating-hadoop-mapreduce-job-with-spring-data-apache-hadoop/

https://github.com/yangboz/spring-data-apache-hadoop-examples/tree/master/mapreduce

http://noushinb.blogspot.com/2013/04/reading-writing-hadoop-sequence-files.html

http://hipi.cs.virginia.edu/gettingstarted.html

package info.smartkit.eip.hadoop;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.PropertySources;
import org.springframework.context.support.ClassPathXmlApplicationContext;


@SpringBootApplication
public class App {

//    public static final boolean IBM_JAVA = JAVA_VENDOR_NAME.contains("IBM");

    public static void main(String[] args) {
//        ApplicationContext ctx = new ClassPathXmlApplicationContext("applicationContext.xml");
        SpringApplication.run(App.class, args);
    }
}
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by jim on 29/6/2017.
 */
public class PropertyFileLoader {
    private InputStream input = null;

    private Properties prop;

    public PropertyFileLoader() {
        prop = new Properties();
        String propertyFileName = "config.properties";
        try {
            ClassLoader cl = this.getClass().getClassLoader();
            input =  cl.getResourceAsStream(propertyFileName);
            prop.load(input);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
     }

    public String getProperty(String name){
        return prop.getProperty(name);
    }

}

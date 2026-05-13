import software.amazon.awssdk.services.eks.model.ComputeConfigRequest;
import java.lang.reflect.Method;
public class TestEks {
    public static void main(String[] args) {
        for (Method m : ComputeConfigRequest.Builder.class.getMethods()) {
            if (m.getName().toLowerCase().contains("pool")) {
                System.out.println(m);
            }
        }
    }
}

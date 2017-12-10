
import com.twitter.hbc.core.HttpConstants;
import com.twitter.hbc.core.endpoint.DefaultStreamingEndpoint;
import com.twitter.hbc.core.endpoint.RawEndpoint;

import java.util.Map;


public class TrendsEndpoint extends RawEndpoint {
    public TrendsEndpoint(String uri, String httpMethod) {
        super(uri, httpMethod);
    }

    public TrendsEndpoint(String uri, String httpMethod, Map<String, String> postParams) {
        super(uri, httpMethod, postParams);
    }
    /*
    public static final String PATH = "/trends/place.json";

    public TrendsEndpoint(boolean backfillable) {
        super(PATH, HttpConstants.HTTP_POST, backfillable);
    }

    public TrendsEndpoint() {
        this(false);
    }
    */


}

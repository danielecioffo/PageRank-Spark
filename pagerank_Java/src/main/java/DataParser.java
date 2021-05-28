import scala.Tuple2;

import java.util.ArrayList;

public class DataParser {

    public static Tuple2<String, ArrayList<String>> parseData(String line) {
        int beginTitleIndex = line.indexOf("<title>") + "<title>".length();
        int endTitleIndex = line.indexOf("</title>");
        String title = line.substring(beginTitleIndex, endTitleIndex);

        int beginTextIndex = line.indexOf("<text");
        int endTextIndex = line.indexOf("</text>");
        String text = line.substring(beginTextIndex, endTextIndex);
        ArrayList<String> outgoingLinks = new ArrayList<>();
        int i = 0;
        while(true) {
            int start = text.indexOf("[[", i);
            if(start == -1) {
                break;
            }

            int end = text.indexOf("]]", start);
            outgoingLinks.add(text.substring(start + 2, end));
            i = end + 1;
        }

        return new Tuple2<>(title, outgoingLinks);
    }
}

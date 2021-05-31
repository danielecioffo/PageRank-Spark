import scala.Tuple2;

import java.util.ArrayList;

public class DataParser {

    public static Tuple2<String, ArrayList<String>> parseData(String line) {
        // get the index of the begin of the title
        int beginTitleIndex = line.indexOf("<title>") + "<title>".length();
        // get the index of the end of the title
        int endTitleIndex = line.indexOf("</title>");
        // get the title
        String title = line.substring(beginTitleIndex, endTitleIndex);

        // get the index of the begin of the text section
        int beginTextIndex = line.indexOf("<text");
        // get the index of the end of the text section
        int endTextIndex = line.indexOf("</text>");
        // get the text section
        String text = line.substring(beginTextIndex, endTextIndex);
        // get all the outgoing_links (any character between 2 pairs of '[]'
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

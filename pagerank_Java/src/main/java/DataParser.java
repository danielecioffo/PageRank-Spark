import java.util.ArrayList;

public class DataParser {

    public static Node parseData(String line) {
        int beginTitleIndex = line.indexOf("<title>") + "<title>".length();
        int endTitleIndex = line.indexOf("</title>");
        String title = line.substring(beginTitleIndex, endTitleIndex);

        int beginTextIndex = line.indexOf("<text");
        int endTextIndex = line.indexOf("</text>");
        String text = line.substring(beginTextIndex, endTextIndex);
        ArrayList<String> outgoingLinks = new ArrayList<>();
        while(true) {
            String initialString = "[[";
            int start = text.indexOf()

        }



        return new Node(title, outgoingLinks);
    }
}

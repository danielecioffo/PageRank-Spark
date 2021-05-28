import java.util.ArrayList;

public class Node {
    private String title;
    private ArrayList<String> outgoingLinks;

    public Node(String title, ArrayList<String> outgoingLinks) {
        this.title = title;
        this.outgoingLinks = outgoingLinks;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public ArrayList<String> getOutgoingLinks() {
        return outgoingLinks;
    }

    public void setOutgoingLinks(ArrayList<String> outgoingLinks) {
        this.outgoingLinks = outgoingLinks;
    }
}

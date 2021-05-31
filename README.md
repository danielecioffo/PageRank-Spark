# PageRank-Spark
Implementation of the MapReduce PageRank algorithm using the Spark framework both in Python and in Java.
 
## How to run the algorithm
Python version: `spark-submit page_rank.py <input file> <output> <number of iterations>`

Java version: `spark-submit --class PageRank <app Jar> <input file> <output> <number of iterations>`

## Input file
The inputs to the program are pages from the Simple English Wikipedia. We will be using a pre-processed version of the Simple Wikipedia corpus in which the pages are stored in an XML format.
The XML file can be found here.

Each page of Wikipedia is represented in XML as follows:

    <title>page name</title>
        ...
    <revisionoptionalVal="xxx">
            ...
            <textoptionalVal="yyy">page content</text>
            ...
    </revision>

The pages have been "flattened” to be represented on a single line. The body text of the page also has all new lines converted to spaces to ensure it stays on one line in this representation.
 Links to other Wikipedia articles are of the form [[page name]].
# PageRank-Spark
Implementation of the MapReduce PageRank algorithm using the Spark framework both in Python and in Java. The documentation for this project can be found [here](documentation/latex/pagerankDocumentation.pdf).
 
## How to run the algorithm
Python version: `spark-submit page_rank.py <input file> <output> <number of iterations>`

Java version: `spark-submit --class PageRank <app Jar> <input file> <output> <number of iterations>`

## Input file
We have tested the algorithm with three input files. 

The first file contains pages from the Simple English Wikipedia. It is a pre-processed version of the Simple Wikipedia corpus in which the pages are stored in an XML format. Each page of Wikipedia is represented in XML as follows:

    <title>page name</title>
        ...
    <revisionoptionalVal="xxx">
            ...
            <textoptionalVal="yyy">page content</text>
            ...
    </revision>

The pages have been "flattened" to be represented on a single line. The body text of the page also has all new lines converted to spaces to ensure it stays on one line in this representation. Links to other Wikipedia articles are of the form [[page name]] and **we considered only links in the _text_ section**.

The other two files contain synthetic datasets we created using the same XML structure with 5000 and 10000 pages, respectively.

All XML files can be found [here](datasets/).

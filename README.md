
This project is to learn the working of Apache Accumulo.

Data is populated into the tables using python script connecting to twitter API and creating two divisions east and west.

The user for managing east and west is different and both will not have access to each other's tables.

This project then calculates the probability of finding the winning team based on the number of mentions on twitter.

From https://accumulo.apache.org/

Apache Accumulo is based on Google's BigTable design and is built on top of Apache Hadoop, Zookeeper, and Thrift. Apache Accumulo features a few novel improvements on the BigTable design in the form of cell-based access control and a server-side programming mechanism that can modify key/value pairs at various points in the data management process.
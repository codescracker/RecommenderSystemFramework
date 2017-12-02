This project is an implementation of item to item recommendation system based on Hadoop. Use MapReduce as computation solution and use HDFS as data storage solution.

The input of the system is the user ratings data, the ouput of the system is the estimated rating of each user for each item. The item with high estimated rating for particular user will be recommended for that user.

**The system contains several steps:**
1. Construct Co-Occurrence matrix from user rating data. The Co-Occurrence matrix is used to represent the relationship between different items.
2. Convert Co-Occurrence matrix into Normalized Co-Occurrence matrix to eliminate the effect of scale. 
3. Finish the matrix multiplication calculation based on Co-Occurrence data and user ratings data to estimate the rating of each item for each user.

**The application of Hadoop in this project makes it possible to process TB level data:**
1. HDFS provides a reliable and fault tolerance data storage solution for TB level data.
2. Map-Reduce enables the parallel computation, distributes the heavy-lift computation to nodes in cluster, provides fault tolerance mechanism and makes the multiplication calculation of million dimension level matrix possible.
 

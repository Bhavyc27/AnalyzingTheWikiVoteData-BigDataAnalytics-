Part-1 Using GraphFrames and Pyspark
This project implements graph analysis using PySpark and GraphFrames, starting from data ingestion, cleaning, and schema creation. The script builds vertex–edge structures, constructs a GraphFrame, and performs tasks such as PageRank, connected components, and degree computation. It also includes data transformations, joins, and visual summaries to understand network relationships. The code forms a complete workflow for scalable big-data graph processing.

Part-2 Scratch Implementation Using Pyspark

This project uses PySpark to perform large-scale graph preprocessing, edge cleaning, and schema reconstruction for network analysis tasks. It loads and prepares raw edge data, handles bidirectional edge expansion, removes duplicates, and builds a structured, optimized graph representation. The code performs transformations such as filtering, joining, aggregation, and caching to enable efficient graph computations. Overall, it forms a scalable workflow for preparing real-world graph datasets for algorithms like PageRank, connected components, and traversal.

Part-3 Using Neo4j

This project uses Neo4j to model, store, and analyze graph data using Cypher instead of PySpark’s distributed framework. The code loads cleaned edge data into Neo4j, creates nodes and relationships, and builds a fully connected property graph. It then performs graph queries such as PageRank, connected components, degree computation, and path exploration directly inside the Neo4j engine. This version showcases how graph algorithms can be executed efficiently through Neo4j’s native graph database and Cypher query optimizations.

Part-4 Using ApacheKafka

This project demonstrates a fault-tolerant data-streaming pipeline built using Apache Kafka, designed specifically for processing large graph datasets in real time. The producer script reads edge data in batches and continuously publishes messages to a Kafka topic while maintaining a checkpoint file that stores the last successfully published index. This ensures that if the producer crashes or restarts, it can resume exactly from where it left off without data loss or duplication. On the other side, the consumer subscribes to the same topic and incrementally rebuilds the graph structure using the streamed edges. It also maintains its own local checkpoint and periodically persists the partially constructed graph, allowing it to recover gracefully from failures and continue ingestion seamlessly. Together, the producer–consumer pair forms a resilient, restart-safe, and scalable pipeline that showcases how Kafka can be used to reliably transport and process graph data for downstream analytics, PageRank computation, and real-time visualization.


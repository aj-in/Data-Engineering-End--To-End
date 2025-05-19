# Data-Engineering-End-To-End
This repository contains the source code of the End-to-End data pipeline built for analyzing transactional records (Synthetic Database)


![image](https://github.com/user-attachments/assets/3ab05fc1-1431-4c06-9518-a740f259aea8)



<ul> <b> <h2>Objectives </h2></b> 

 <li>Implemented Medallion architecture (Bronze, Silver, Gold) for data pipelines. </li>
 <li>Pulled data from Azure Data Lakes into Databricks transformed it and sent it to further layers in the Medallion Architecture.  </li>


 <li>Used Star Schema Architecture for ETL (Gold Layer) with "Orders" as the <b><i>Fact table</i> </b>u and "Customers", "Products" and "Regions" as <b><i>Dimension Table.</i> </b> </li>
 <li>Converted CSV to parquet file formats for faster querying. </li>
 

 <li>Developed PySpark code with utility functions that transformed and processed data. </li>
 <li>Improved familiarity with Data processing in cloud environments (Azure). </li>

</ul>


<br>

Execution:

Running this code on your machine would require significant conficuration so I am attaching a few of the screenshots of how the execution should look like


<b>All of the resources created in Microsoft Azure </b>

![Azure](https://github.com/user-attachments/assets/b7539177-9074-4c96-ae89-755ac0780478)
<b>Data Transformation </b>

![image](https://github.com/user-attachments/assets/1e7c199e-6352-445b-b189-4dfdecdfcfbd)

<b>Delta Live Tables </b>

![image](https://github.com/user-attachments/assets/c0a76e57-9362-468a-a7ff-195fbac6363a)



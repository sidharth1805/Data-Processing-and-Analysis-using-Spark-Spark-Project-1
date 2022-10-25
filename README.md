### Data Processing and Analysis using Spark | Spark Project-1

![](https://cdn-images-1.medium.com/max/1284/1*rOe5v0px2ooR-uS1jMksTg.png)

Overview of the Design

In this blog post, we will understand how to perform simple operations on top of a relational database to get valuable Insights using Apache Spark.

----------

#### Problem Statement:

We have three data tables which are of type CSV. We are performing basic joins in those tables and Creating a Demoralized data frame so that we could perform some processing and Analytics on top of our Data.

#### Dataset:

For this project, we are going to use the [Retail Dataset](https://github.com/sidharth1805/Data-Processing-and-Analysis-using-Spark-Spark-Project-1/tree/main/Data). These Datasets consist of three tables customer, orders, and order_items.

#### Tech Stack / Skill used:

1.  Spark
2.  SQL

#### Setting the workspace:

We will use Databricks Community Edition which is free of cost to perform the Spark operations if you prefer to use spark locally or in the Hadoop cluster it’s fine.

Refer: [https://www.databricks.com/product/faq/community-edition](https://www.databricks.com/product/faq/community-edition)

After setting up the workspace create a cluster and Open a workbook. You are all set to go.

#### Project:

Now it's time to add our data to the Databricks.

![](https://cdn-images-1.medium.com/max/1284/1*3luM6g9xUS6mSd8LrM6Grg.png)

Upload the Data by creating proper folders in this below example I have created a folder Data under which I created a folder orders and uploaded the Data File.

![](https://cdn-images-1.medium.com/max/1284/1*q1y2jq4HCyl3G6DzXwVcQA.png)

![](https://cdn-images-1.medium.com/max/1284/1*03P0lXF0JXsLNO6sMzd0nA.png)

Now That we are done with Setting up the Data in databricks now it's time to write some code.

**Step 1:** Check if all the required files are placed

%fs ls dbfs:/FileStore/tables/Data

![](https://cdn-images-1.medium.com/max/1284/1*O4S_5uileBxtBJEG6XShww.png)

**Step 2:** Create Dataframes for orders,order_items, and customers. The CSV files we are using don't have a schema hence while creating the data frame we define the schema.

**Step 3:** View DataFrame

order_items_df.show(2)

![](https://cdn-images-1.medium.com/max/1284/1*fLwzJ0n53JYNF_HvJ95TOg.png)

**Step 4:** Join Our Tables into a new DataFrame(oreder_details) to create a Denormalized data frame.

-   Joining customers and orders table initially.

customers_orders_df=customer_df.join(orders_df,customer_df['customer_id']==orders_df['order_customer_id'])

-   Project the required Data using SELECT clause.

customers_orders_df.select('customer_id','order_id','order_date','order_status').orderBy('customer_id').show(10)

![](https://cdn-images-1.medium.com/max/1284/1*ld6Tas61Cx_n5zjXYAoEew.png)

-   Consolidating order_id,order_date, and order_status to structure data type.

from pyspark.sql.functions import struct

customers_orders_df.select('customer_id',struct('order_id','order_date','order_status').alias('order_details')).orderBy('customer_id').show(10)

![](https://cdn-images-1.medium.com/max/1284/1*dsrz0kso_cnF3hg8NEttNA.png)

-   Generate an array of struct. field using order_details. Here we are grouping the customer_id and storing the order_details in form of an array.

customer_order_struct=customers_orders_df.select('customer_id',struct('order_id','order_date','order_status').alias('order_details'))

from pyspark.sql.functions import collect_list

final_df=customer_order_struct.groupBy('customer_id').agg(collect_list('order_details').alias('order_details')).orderBy('customer_id')

![](https://cdn-images-1.medium.com/max/1284/1*PHJ6r19u5SVJXtMxmfgkWg.png)

**Step 5:** Export Data Frame into a JSON File.

final_df.coalesce(1).write.json('dbfs:/FileStore/tables/Data/final')

![](https://cdn-images-1.medium.com/max/1284/1*opuVNdwkT9jE2fkByiQu2g.png)

Previously we had performed the Denormalization for orders and customers. Now we will perform for the entire three tables.

Export Data Frame into a JSON File.

denorm_df.coalesce(1).write.json('dbfs:/FileStore/tables/Data/denorm')

----------

Now we have the required data to do our analysis. Now it's time to analyze the Denormalized data using Spark.

We shall perform the below analysis on our data

1.  Get the Details of the order placed by the customer on 2014 January 1st
2.  Compute the monthly customer Revenue

#### Problem Statement — 1:

Read the Data Frame.

json_df=spark.read.json('dbfs:/FileStore/tables/Data/denorm/part-00000-tid-4357456608139543307-49cdb4fe-37a2-4435-be01-b6711f29eb3d-211-1-c000.json')

json_df.show(2)

![](https://cdn-images-1.medium.com/max/1284/1*dlMPVpTiJx4wGwB2BX7kGQ.png)

json_df.select('customer_id','customer_fname',explode('order_details').alias('order_details')). \  
filter('order_details.order_date LIKE "2014-01-01%"'). \  
orderBy('customer_id'). \  
select('customer_id','customer_fname','order_details.order_id','order_details.order_date','order_details.order_status'). \  
show(10)

![](https://cdn-images-1.medium.com/max/1284/1*BoRk9EEsk3zj68o5G8gfcA.png)

#### Problem Statement — 2:

-   To calculate the monthly customer revenue we need to perform aggregations(SUM) on order_item_subtotal from the order_items table.
-   In our input data, we have wrapped all the details into a struct data type Hence it's time to flatten all the details.

![](https://cdn-images-1.medium.com/max/1284/1*ccilDd7dyxW3Cp44lk7COw.png)

-   After flattening our data let's write the logic to get the monthly revenue

![](https://cdn-images-1.medium.com/max/1284/1*2WssNX_in6Yy_JwDSeBvNA.png)

----------

Hope this small project will give you the required understanding of how to denormalize Data tables and use Spark to perform analysis on top of the data.

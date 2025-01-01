
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,year,month,quarter,udf,lit,countDistinct,count,date_format,collect_set,current_date,max,day,expr,sum,ntile,concat
from pyspark.sql.types import *
from pyspark.sql.window import Window
import plotly.express as px
import streamlit as st

spark=SparkSession.builder.getOrCreate()

sales_schema=StructType([ StructField("S_Product_id",IntegerType(),False),
                          StructField("Customer_id",StringType(),False),
                          StructField("Order_date",DateType(),True),
                          StructField("Location",StringType(),True),
                          StructField("Source_order",StringType(),True)
                   ])

menu_schema=StructType([ StructField("M_Product_id",IntegerType(),False),
                          StructField("Product_name",StringType(),False),
                          StructField("Price",FloatType(),True),
                   ])

sales_df=spark.read.format("csv").schema(sales_schema).load(path="/content/sales.csv.txt",inferSchema=True)
menu_df =spark.read.format("csv").schema(menu_schema ).load(path="/content/menu.csv.txt" ,inferSchema=True)

df=sales_df.join(menu_df,on=sales_df.S_Product_id==menu_df.M_Product_id,how="inner").drop("M_product_id")

df=df.withColumns( {"Year":year("Order_date"),"Month":month("Order_date"), "Quarter":quarter("Order_date") })
df=df.dropDuplicates()

##############################################################################

st.set_page_config(layout="wide")

html_temp = """
            <div style="background-color:tomato;padding:10px">
            <h1 style="color:white;text-align:center"> Sales Analysis.</h1>
            </div>
                   """
st.markdown(html_temp,unsafe_allow_html=True)


col1,col2=st.columns(2)
with col1:

          x=df.groupBy("Source_order").sum("Price").withColumnRenamed("sum(Price)","Total Sales")
          fig=px.pie(data_frame=x,
                    names="Source_order",
                    values="Total Sales",
                    color="Source_order",
                    color_discrete_sequence=["red","green","blue"],
                    title="Total Sales for each Source"
          )
          fig.update_traces(textinfo="label+percent",textposition="outside")
          st.plotly_chart(fig,use_container_width=False)

          x=df.groupBy("Product_name").sum("Price").withColumnRenamed("sum(Price)","Total Sales").sort(col("Total Sales").desc())
          fig=px.bar(data_frame=x,
                     x="Total Sales",
                     y="Product_name",
                     color="Product_name",
                     color_discrete_sequence=["red","blue","green","yellow","gray","orange"],
                     text="Total Sales",
                     title="Total Sales for each Product",
                     orientation="h"
           )
          st.plotly_chart(fig,use_container_width=True)

          x=df.groupBy("Customer_id").sum("Price").withColumnRenamed("sum(Price)","Total Payment")
          fig=px.bar(data_frame=x,
                    x="Total Payment",
                    y="Customer_id",
                    color="Customer_id",
                    color_discrete_sequence=["red","gray","blue","green","yellow"],
                    text="Total Payment",
                    title="Total Payment for each Customer",
                    orientation="h",
                    category_orders={"Customer_id":["A","B","C","D","E"]}
           )
          st.plotly_chart(fig,use_container_width=True)


          x=df.groupBy("Location").sum("Price").withColumnRenamed("sum(Price)","Total Sales")
          fig=px.bar(data_frame=x,
                     x="Location",
                     y="Total Sales",
                     color="Location",
                     color_discrete_sequence=["orange","red","blue"],
                     text="Total Sales",
                     title="Total Sales for each Country",
                     labels={"Location":"Country"}
           )
          st.plotly_chart(fig,use_container_width=True)


with col2:

          x=df.groupBy("Source_order","Year").agg(countDistinct("Order_date").alias("count")).orderBy("count").withColumn("Year",col("Year").cast("string"))
          fig=px.bar(data_frame=x,
                      x="count",
                      y="Source_order",
                      color="Year",
                      color_discrete_sequence=["blue","red"],
                      text="count",
                      title="Most Visited Source Order",
                      orientation="h",
                      barmode="relative"
           )
          st.plotly_chart(fig,use_container_width=True)

          x=df.groupBy("Product_name","Year").count().orderBy("count").withColumn("Year",col("Year").cast("string"))
          fig=px.bar(data_frame=x,
                    x="count",
                    y="Product_name",
                    color="Year",
                    color_discrete_sequence=["red","blue"],
                    text="count",
                    title="Best Selling Product",
                    orientation="h",
                    barmode="relative"
           )
          st.plotly_chart(fig,use_container_width=True)

          x=df.groupBy("Customer_id").agg(countDistinct("Order_date").alias("Number of Visits"),count("Customer_id").alias("Number of Orders")).sort("Customer_id")
          fig=px.area(data_frame=x,
                    x="Customer_id",
                    y=["Number of Visits","Number of Orders"],
                    color_discrete_sequence=["red","green"],
                    title="Comparison between Customer Visits to their Orders"
            )
          st.plotly_chart(fig,use_container_width=True)

          x=df.groupBy("Location").count()
          fig=px.bar(data_frame=x,
                    x="Location",
                    y="count",
                    color="Location",
                    color_discrete_sequence=["orange","red","blue"],
                    text="count",
                    title="No of Orders in each Country",
                    labels={"Location":"Country"})
          st.plotly_chart(fig,use_container_width=True)


c1,c2,c3=st.columns(3)

with c1:
          x=df.groupBy("Year").sum("Price").withColumnRenamed("sum(Price)","Total Sales").withColumn("Year",col("Year").cast("string"))
          fig=px.bar(data_frame=x,
                    x="Year",
                    y="Total Sales",
                    color="Year",
                    text="Total Sales",
                    title="Yearly Sales",
                    category_orders={"Year":["2022","2023"]},
                    color_discrete_sequence=["blue","red"]
           )
          st.plotly_chart(fig,use_container_width=True)


with c2:
          x=df.groupBy("Quarter").sum("Price").withColumnRenamed("sum(Price)","Total Sales").withColumn("Quarter",col("Quarter").cast("string"))
          fig=px.bar(data_frame=x,
                    x="Quarter",
                    y="Total Sales",
                    color="Quarter",
                    color_discrete_sequence=["blue","red","gray","green"],
                    text="Total Sales",
                    category_orders={"Quarter":["1","2","3","4"]},
                    title="Quarterly Sales"
                      )
          st.plotly_chart(fig,use_container_width=True)

with c3:
          x=df.withColumn("Month_name", date_format( col("order_date") ,"MMMM")).groupBy("Month","Month_name").sum("Price").withColumnRenamed("sum(Price)","Total Sales").sort(col("Month").asc())#.withColumn("Month",col("Month").cast("string"))
          fig=px.bar(data_frame=x,
                    x="Month",
                    y="Total Sales",
                    text="Total Sales",
                    hover_name="Month_name",
                    title="Monthly Sales",
                    color_discrete_sequence=["red"]
                      ) 
          st.plotly_chart(fig,use_container_width=True)

current_date="2024-1-1"
RFM=df.groupBy("Customer_id").agg(     ( current_date-max("Order_date")  ).alias("Recency") ,
                                                    count("Customer_id").alias("Frequency") ,
                                                             sum("Price").alias("Monetory")
                                 ).sort("Customer_id").withColumn("Recency",col("Recency").cast("integer"))

RFM=RFM.withColumns({ "Recency_Score":   ntile(3).over( Window.orderBy( col("Recency").desc() ) ),
                      "Frequency_Score": ntile(3).over( Window.orderBy( col("Frequency").asc() ) ),
                      "Monetory_Score":  ntile(3).over( Window.orderBy( col("Monetory").asc() ) ),
                 })
RFM=RFM.withColumn( "RFM_Score",  col("Recency_Score")+col("Frequency_Score")+col("Monetory_Score") ) 
@udf(returnType=StringType())
def segment(rfm):
    if rfm < 5:
      return "Hibernating Customer"
    elif rfm < 7:
      return "Need Attention"   
    else:
      return "VIP Customer"

RFM=RFM.withColumn("Segment", segment(col("RFM_Score")))
fig=px.treemap(data_frame=RFM,
               path=["Segment","Customer_id"],
               color="Segment",
               color_discrete_sequence=["blue","green","red"],
               title="Customer Segmentation by RFM Analysis"
               )
st.plotly_chart(fig,use_container_width=True)

# Introduction

This repo is my **first project on Apache Spark with Scala**. It is focus on learning how to process some source files 
into a EDW. In order to do so, a sketch of a Data Lakehouse has been created:
- **_Data Ingestion Layer:_** it is the integration point from the external data providers to the Data Lakehouse. As 
it is not a real project, no data providers exists. To overcome this inconvenience, the subsection of the code 
**context.building** contains code to create fake source files (for more information go to section _Source Data_) 
and to replicate the call on demand of the ETL process (go to section _How to execute the code_).
- **_Data Lake Layer:_** it is where data is stored. It contains raw, intermediate,
processed and archived data. As this first project on Apache Spark has been designed to focus on learning to code, this 
layer has been created as a simple folder structure into the repo instead of a bunch of buckets on the cloud.
- **_Data Processing Layer:_** it takes care of transforming the data, so it can be consumed for insights. This is the 
main focus of this project, how to develop code in Apache Spark with Scala to populate raw data from a source into the
EDW. The pipeline coding can be found on **retailsales** (for more information go to section _Pipeline_). 
- **_Data Analytics Layer:_** it is a playground for analysts, data scientists, and BI users to create reports, perform
analysis, and experiment with AI/ML models. AS NO ACCESS TO SOLUTION SUCH AS BABLABALA WE WILL CREATE A SMALL AMOUNT OF FILES TO EXPLORE THE SOLUTION
- **_Data Serving Layer:_** it takes care of serving the processed data to data consumers. 
- **_Data Governance Layer:_** it takes care of the data governance at the Data Lakehouse. In a sense, this 
documentation is already serving this purpose. Nonetheless, a set of policies (for more info go to section 
_Data Governance Policy_) an a data catalog (for more information go to section _Data Catalog_) has been created.
- **_Data Security Layer:_** it takes care of Security at the Data Lakehouse.

## Data Governance Policy
- Raw, temp and processed data are saved on different buckets. A bucket (_on this context_) is a folder hanging from the
  _data_lake_ folder.
- At the raw bucket, the top hierarchy level is the nature of the source (if it is internal or external). Next one is 
the source name and the third the topic.  
- Data at the EDW is saved under the next schema pattern:
  - Tables needed for ETL purposes, are saved under ETL schema.
- Name convention:
  - Use meaningful names. Always choose boring meaningful long names over short smart names. 
  - For buckets and folder names use snake_case.
  - For EDW schema and table names use snake_case.
  - For fields names in tables and views at the EDW use CamelCase.
  - For Scala:
    - Functions and variables: camelCase.
    - Classes and Objects: CamelCase.
- The EDW will follow Dimensional Modelling architecture.
- All pipelines will have the next suite test:
  - Unit testing and Unit Integration Testing develop with ScalaTest and sample data provided by source data providers.
  - A fake creator of sources to be able to populate several test iterations.
    
## Data Catalog
This solution deals with EPOS data from supermarkets. Data sources for EPOS data can be either directly provided by 
supermarkets or a data broker. The data provided is aggregated by week, retailer and product and include measures such 
as number of units sold and the amount of money obtained from those sales. All data is delimited to the United Kingdom,
so currency is the British Pound (GBP).
Also, a SKU mapper is provided by the internal department interested on this data to improve insight.

### Source Data
There are 2 type of EPOS source files digested into the EDW:  
1. MAT: it is an imaginary data broker that provides EPOS data for Tesco, Marks and Aldi on a weekly basis. File format 
is *.csv. The raw files are saved into the Data Lake on the next path: _raw/external/mat/{week_ending}_. 
The source file columns are as follows:

| Column Name | Data Type | Description                                                                            | Sample     |
|-------------|-----------|----------------------------------------------------------------------------------------|------------|
| SKU         | Integer   | Unique product identifier. It can be different between retailers                       | 100112     |
| Retailer    | String    | Name of the Retailer                                                                   | Tesco      |
| Week Ending | Date      | Cumulative fields are for the week ended on the indicated date. Week end is on Sundays | 02/01/2022 |
| Units       | Float     | Number of Units sold                                                                   | 330.0      |
| Sales       | Float     | Amount of Pounds obtain for the sales                                                  | 480.21     |

2. **_THIS IS YET TO BE WRITTEN_**

The **_SKU Mapper_** is an ETL resource that is populated into: _edw/etl/sku_mapper_ and 
_raw/internal/Marketing/sku_mapper_. The columns are as follows:

| Column Name | Data Type | Description                                                                            | Sample     |
|-------------|-----------|----------------------------------------------------------------------------------------|------------|
| SKU         | Integer   | Unique product identifier. It can be different between retailers                       | 100112     |
| Retailer    | String    | Name of the Retailer                                                                   | Tesco      |
| Description | String    | Meaningful name for a product                                                          | Coca Cola  |
| ProductType | String    | Type of a product                                                                      | 330.0      |

### EDW - Data Model
The conceptual data model for the EDW is as depicted on the picture below.

The Date dimension is created one-off with the object **DateDimensionBuilder** found at 
_src/main/scala/date/DateDimensionBuilder_

## How to execute the code
Talk here about ApiFake.

### Pipelines 
The Data Processing Layer is composed of the next pipelines:
- EPOS family:
  - **Mat Pipeline:** it is to populate data provided by the data broker MAT as indicated on the section _Source Data_.
  The pipeline code can be found on _class MatPipeline_(). The entry point to this pipeline is 
  - **asdasd**
- Date Pipeline: 


## Test Environment
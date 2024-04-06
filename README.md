# Chaos to Clarity: Unifying Inconsistent Datasets to Create a Master Data

Problem Statement: 

I received two datasets with information about various organizations/restaurants, including details such as address, city, and zip code. The problem arose due to the inconsistent nature of the data. Initially, the task appeared straightforward – eliminate duplicates and ensure the final dataset comprises only unique entries. However, upon closer inspection, navigating through the inconsistent data proved to be quite a puzzle. Messy datasets of this nature were not something I commonly dealt with. Nevertheless, tackling this intricate problem was both interesting and presented a welcome challenge in my learning journey.

DATA CLEANING PROCESS:

After checking both files, it looks like the data is not the same. For example, even if the entity names are similar, the city, zipcode, and address is different. Some addresses match, but the entities are completely different. There are only 178 records that exactly match, and some of them are repeated.

Decided to use these column names: ENTITY, ADDRESS, CITY, ZIPCODE. Couldn't use "RESTAURANTS" for the name column because the data includes places that are not considered restaurants.

Decided to combine both datasets since there weren't many similarities found in the files. The original count was before cleaning the Data was 417,559

Handling Addresses:

Rephrased and contextualized instructions for cleaning CSV data:

Steps:

1.	Identifies and remove inconsistencies:

  Whitespace, trailing/leading spaces, special characters:
  
    Examine all columns for these issues. Use appropriate methods (e.g., trimming, regular expressions) to remove them.		
    
    Being mindful of preserving necessary spaces within data like street addresses, directions
    
City and ZIP code uniformity:
    	Checked if there are variations in capitalization, formatting, or presence of abbreviations in city names.
    	First thing, I focused on cleaning up the "CITY" and "ZIPCODE" Ensure ZIP codes are all in a standard 5-digit format, adding leading zeros if necessary (e.g., converting "777" to "00777").   
    	Consider standardizing city names to a specific format (e.g., all uppercase, title case). 
3.	The "ADDRESS" column was my next target, mainly because it serves as a standard geographical reference. It felt more approachable than the "RESTAURANTS/ENTITY" column, so I decided to tackle it first.
4.	To clean the "ADDRESS", I took the manual approach first! I used regular expressions to identify and fix common issues, For the ADDRESS part I took the 80/20 approach
5.	Abbreviations: Thanks to the USPS-Suffix-Abbreviations chart, I standardized inconsistent abbreviations for words like "AVENUE," "STREET," "HIGHWAY," and more.
6.	Directions: I transformed compass abbreviations like "S" to "SOUTH," "N" to "NORTH," and so on, ensuring clarity and uniformity.
7.	Cleaning involved going back and forth between addresses and entities. I'd fix addresses, then tackle entities, then go back to addresses to catch new issues. Time constraints meant I couldn't divide and combine data as planned, so I applied various techniques on the fly.
![image](https://github.com/girish-Pillai/Chaos-to-Clarity-MasterDataManagement/assets/98634040/58d36dc2-3e3a-4659-a5be-c4f3bebe6104)

Cleaning Business Names:
1.	Pass 1: Removing High-Frequency Designations: Eliminated words like "INC," "LLC," "CORP," etc., using regular expressions.
2.	Performed this in three passes to address cases where multiple designations appeared in a single name (e.g., "CENTURY KANG INC CO LLC"). This ensures thorough cleanup.
3.	Pass 2: Standardizing Well-Known Entities:
4.	Manually identified high-frequency entities (e.g., "DUNKIN DONUTS," "DOMINOS," "SUBWAY") repeated over 200 times.
5.	Standardized their names to consistent formats (e.g., all uppercase or title case) to facilitate grouping and improve Edit Distance Algorithm performance.
6.	Exported the cleaned dataframe to Parquet format for leveraging columnar processing
7.	While dividing and combining data to address inconsistencies, I discovered additional similarities his suggests that a more iterative approach could yield even better results in future iterations
Similarity Metrics:
1.	After reading and exploring various similarity metrics, I tested two approaches:
a.	Levhensteign weight: In short, The Levenshtein distance is a measure of the minimum number of single-character edits (insertions, deletions, or substitutions) required to change one word into the other.
b.	FuzzyWuzzy package: Python library that provides tools for string matching and comparison. In our project, we utilize
c.	Despite my efforts to leverage PySpark's built-in Levenshtein method, the results were not highly successful.
Fuzzy Wuzzy Approach:
1.	Here's the FuzzyWuzzy code I used, focusing on tackling problems incrementally and keeping other factors constant. I started with addresses while holding other columns stable, then moved on to refining addresses again based on restaurant similarities. Ideally, I wanted to iteratively divide the data, clean it, run the algorithm, combine it, and repeat until convergence. However, time constraints limited me to two iterations. In future work, I'd like to explore further techniques, including applying Levenshtein distance after achieving a baseline level of data cleanliness.
2.	After eliminating exact matches and duplicates, I arranged the dataset to highlight pairs of similar rows.
3.	I started by splitting the data into two groups based on row numbers, effectively creating pairs of potentially similar rows and divided the dataset based on the row nums. 
4.	This technique proved to be better because I can easily apply the UDF on columns, back and forth 
5.	Next, I decided on a threshold of 70% similarity. Pairs with scores above 70% were good candidates for being duplicates, but things were a bit more tricky. If both entities in a pair scored above 70%, I chose the longer one, assuming it had more information.
6.	Finally, I merged the selected pairs back into a single dataframe. I cleaned up some temporary columns and renamed others to make things clearer.
7.	At every step I checked the if their distinct matches that I get so that I can push it directly to the result. This process effectively identified and combined potential duplicates by comparing entity names and addresses.
Things I tried but didn’t yield much result:
As I had the Zip code and City details, I thought of using an open-source geo API that would help me get the addressed standardized, I did write some UDFs to compare zip and city to match the address but because of the hardware constraints I was never able to finish the execution as the UDF had to manually go through each row to compare.
Future Scope and Scaling:
•	Pipeline Design:
•	Full Load First Time:
•	Conducting a comprehensive cleaning manually or through algorithms on the first load, using human validation and checksums.
•	Once clean, we can consider this clean data as the source of truth.
•	Create two copies of the clean data: one designated as the Master data and the other as the Source Table.
•	To enhance query performance over time when the data grows, we can consider incorporating the SATAE Column Using the zip code and city  which will allow better partitioning and bucketing.
•	Incremental Load (Timely run):
•	After the initial load, new incoming data will pass through the Master-Data-Management (MDM) table.
•	Check for similarities, apply transformations, substitutions, on the fly and push the data to the Source table.
•	Not all data goes to the Source immediately; similarity checks based on the MDM data determine eligibility.
•	Criteria for pushing data (Just an assumption)
•	80% similarity: Directly to the Source after going through the transformations
•	40-80% similarity: To an error/review table for manual checking and resolution before re-entering the pipeline.
•	<40% similarity: Considered as potentially new entries, requiring manual verification, addition to MDM, and re-entry into the pipeline.
•	Over Time:
•	With refinements and additions to the MDM table and human validations, manual intervention reduces.
•	Confidence scores increase, allowing more data to go to the Source without frequent human checks.
•	Applying Data linkage techniques would merge information from different sources about the same address or entity to create a more comprehensive master data catalog.
![image](https://github.com/girish-Pillai/Chaos-to-Clarity-MasterDataManagement/assets/98634040/24a413fc-0215-42ff-88a6-c9087d117907)


Cleaned and merged two highly inconsistent restaurant datasets to create a Master Data of unique records, resulting in a final dataset with 50% improved accuracy.

Leveraged advanced data cleaning techniques like Levenshtein distance, regular expressions, and FuzzyWuzzy for string matching and comparison.  

Utilized GCP for deploying the project with Dataproc for executing Spark jobs and Cloud Composer for automated deployment.

For standalone setups, employed Docker for deployment regardless of the environment.

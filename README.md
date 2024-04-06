# Chaos to Clarity: Unifying Inconsistent Datasets to Create a Master Data
### Problem Statement:
I received two datasets with information about various organizations/restaurants, including details such as address, city, and zip code. The problem arose due to the inconsistent nature of the data. Initially, the task appeared straightforward – eliminate duplicates and ensure the final dataset comprises only unique entries. However, upon closer inspection, navigating through the inconsistent data proved to be quite a puzzle. Messy datasets of this nature were not something I commonly dealt with. Nevertheless, tackling this intricate problem was both interesting and presented a welcome challenge in my learning journey.

### Data Cleaning Process:
After checking both files, it looks like the data is not the same. For example, even if the entity names are similar, the city, zipcode, and address is different. Some addresses match, but the entities are completely different. There are only 178 records that exactly match, and some of them are repeated.
Decided to use these column names: ENTITY, ADDRESS, CITY, ZIPCODE. Couldn't use "RESTAURANTS" for the name column because the data includes places that are not considered restaurants.
Decided to combine both datasets since there weren't many similarities found in the files. The original count was before cleaning the Data was 417,559.

### Handling Addresses:
Rephrased and contextualized instructions for cleaning CSV data:

#### Steps:
1. Identifies and remove inconsistencies:
   - Whitespace, trailing/leading spaces, special characters:
     - Examine all columns for these issues. Use appropriate methods (e.g., trimming, regular expressions) to remove them.
     - Being mindful of preserving necessary spaces within data like street addresses, directions.
   - City and ZIP code uniformity:
     - Checked if there are variations in capitalization, formatting, or presence of abbreviations in city names.
     - First thing, I focused on cleaning up the "CITY" and "ZIPCODE" Ensure ZIP codes are all in a standard 5-digit format, adding leading zeros if necessary (e.g., converting "777" to "00777").
     - Consider standardizing city names to a specific format (e.g., all uppercase, title case).
2. The "ADDRESS" column was my next target, mainly because it serves as a standard geographical reference. It felt more approachable than the "RESTAURANTS/ENTITY" column, so I decided to tackle it first.
3. To clean the "ADDRESS", I took the manual approach first! I used regular expressions to identify and fix common issues. For the ADDRESS part, I took the 80/20 approach.
4. Abbreviations: Thanks to the USPS-Suffix-Abbreviations chart, I standardized inconsistent abbreviations for words like "AVENUE," "STREET," "HIGHWAY," and more.
5. Directions: I transformed compass abbreviations like "S" to "SOUTH," "N" to "NORTH," and so on, ensuring clarity and uniformity.
6. Cleaning involved going back and forth between addresses and entities. I'd fix addresses, then tackle entities, then go back to addresses to catch new issues. Time constraints meant I couldn't divide and combine data as planned, so I applied various techniques on the fly.


### Cleaning Business Names:
1. **Pass 1: Removing High-Frequency Designations:**
   - Eliminated words like "INC," "LLC," "CORP," etc., using regular expressions.
2. **Pass 2: Standardizing Well-Known Entities:**
   - Manually identified high-frequency entities (e.g., "DUNKIN DONUTS," "DOMINOS," "SUBWAY") repeated over 200 times.
   - Standardized their names to consistent formats (e.g., all uppercase or title case) to facilitate grouping and improve Edit Distance Algorithm performance.
3. **Export to Parquet Format:**
   - Exported the cleaned dataframe to Parquet format for leveraging columnar processing.
4. **Iterative Approach:**
   - Discovered additional similarities while dividing and combining data, suggesting that a more iterative approach could yield even better results in future iterations.

### Similarity Metrics:
1. **Levenshtein Distance:**
   - Tested the Levenshtein distance as a measure of similarity, but results were not highly successful.
2. **FuzzyWuzzy Package:**
   - Utilized the FuzzyWuzzy package in Python for string matching and comparison.

### Fuzzy Wuzzy Approach:
1. **Incremental Approach:**
   - Tackled problems incrementally, focusing on addresses first and gradually refining based on restaurant similarities.
2. **Pairwise Comparison:**
   - Split the data into pairs of potentially similar rows based on row numbers.
   - Applied a threshold of 70% similarity for identifying potential duplicates.
   - Selected the longer entity name if both scored above 70% similarity.
   - Merged selected pairs back into a single dataframe after cleaning up temporary columns.

### Future Scope and Scaling:
- **Pipeline Design:**
  - Conduct a comprehensive cleaning manually or through algorithms on the first load, using human validation and checksums.
  - Enhance query performance over time by incorporating the SATAE Column Using the zip code and city for better partitioning and bucketing.
- **Incremental Load:**
  - Apply transformations, substitutions, and similarity checks on new incoming data before pushing it to the Source table.
  - Establish criteria for pushing data based on similarity scores.
- **Over Time:**
  - Refine and add to the Master-Data-Management (MDM) table to reduce manual intervention.
  - Increase confidence scores to allow more data to go to the Source without frequent human checks.
- **Data Linkage Techniques:**
  - Merge information from different sources about the same address or entity to create a more comprehensive master data catalog.


### How is this project deployed
- Utilized GCP for deploying the project with Dataproc for executing Spark jobs and Cloud Composer for automated deployment.

- For standalone setups, employed Docker for deployment regardless of the environment.

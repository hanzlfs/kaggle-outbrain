## Data EDA and feature engineer


- EDA and Data Cleaning
	- Parsing
		- We can parse geo_location into three columns: country, state, DMA

	- Missing data
		- missing categorical variables -> fill with '-999' or 'UNKNOWN'
		- missing timestamp -> fill with **?**

	- Correlation analysis
		- Target vs. predictor distribution
		For example, collect two groups in train, one group with clicked display-ad (1) and the other unclicked(0),
		plot the box plot for a selected feature below for each group and compare.

#### First set of features

- Feature extraction

	- **1.** display_id -> document features

	```
	create table display_doc_features
	from
	train/test
	left join events
	on display_id
	left join documents_meta, document_entities, document_topics, document_categories
	on document_id
	```

	- Categorical
		- [x] documents_meta.csv -> source_id
		- [x] documents_meta.csv -> publisher_id
	- Time
		- [x] documents_meta.csv -> publish_time **?**
	- Summary
		- [x] document_entities -> entity_id with max(confidence_level)
		- [x] document_topics -> topic_id with max(confidence_level)
		- [x] document_categories -> category_id with max(confidence_level)

	- **2.** ad_id -> document features (same features as in **1** with the following join)

	```
	create table ad_doc_features
	from train/test
	left join promoted_content
	on ad_id
	left join documents_meta, document_entities, document_topics, document_categories
	on document_id
	```

	- **3.** ad_id features

	```
	create table ad_features
	from train/test
	left join promoted_content
	on ad_id
	```

	- Categorical
		- [x] promoted_content -> campaign_id
		- [x] promoted_content -> advertiser_id

	- **4.** display_id features

	```
	create table display_features
	from train/test
	left join events
	on display_id
	```

	- Categorical
		- [x] events -> platform
		- [x] events -> geo_location
	- Time
		- [x] events -> timestamp -> 0,1,...,23 hours in a day, 0,1,...,6 day in a week

	- **5.** uuid-document_id features in page_view

	```
	create table uu-doc-features
	from train/test
	left join events
	on display_id
	left join page_view
	on unique uuid - document_id
	```

	- Summary
		- [x] page_view -> platform -> number of visits in each value / time since first day
		- [x] page_view -> geo_location -> number of visits in each value / time since first day
		- [x] page_view -> traffic_source -> number of visits in each value / time since first day
		- [x] page_view -> timestamp -> number of visits in past (24 hrs, 2 days, 1 week, etc)
		- [x] page_view -> timestamp -> number of visits / time since first day
		- [x] page_view -> timestamp -> average number of visits in each 0-23 hours, each 0-6 weekday

	- **6.** uuid features in page_view (do the same thing as in **5** on uuid level only)

	```
	create table uu-features
	from train/test
	left join events
	on display_id
	left join page_view
	on unique uuid
	```
#### Explore more on page-views to profile display_id
- **1 from events->uuid in page_views**
	- [X] 1 number of total visits
	- [x] 2 average daily number of total visits = num_visits / day since 0614
	- [x] 3 number of visits in the previous 1 day, 3 days, 7 days, etc from the timestamp in events
	- [x] 4 number of unique documents viewed
	- [x] 5 frequency of viewed document topI topic/category/entity = number of topI/category/entity / num_visits
	- [x] 6 frequency of visits in each weekday/hour = number of visits in each weekday/hour  /  num_visits
	- [x] TBD ...

- **2 from events->docid in page_views**
	- [x] 1 number of total appearances of this doc in page_view = num_viewed
	- [x] 2 number of unique users viewed this doc
	- [x] 3 average daily number of views = num_views / day since 0614
	- [x] 4 number of views in previous 1 day, 3 days, 7 days, etc from events timestamp
	- [x] 5 frequency of view in each weekday/hour = number of views in each weekday/hour  /  num_viewed
	- [x] TBD ...

- **3 from events->uuid->docid->similar uuid **
	- [x] for all uuid that viewed this doc, take the average of features in **1**

- **4 from events->docid->uuid->similar docid**
	- [X] already included in 1.5, pass
	- [x] for all docs that viewed by this uuid, take the average of features in **2**

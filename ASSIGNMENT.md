# S3 Exercise Datasets

All datasets are publicly accessible at `s3://dbx-class-exercise-datasets/`

---

## Climate Change

Global temperature data from 1750 onwards.

| File | Size |
|------|------|
| `GlobalTemperatures.csv` | 201 KB |
| `GlobalLandTemperaturesByCountry.csv` | 22 MB |
| `GlobalLandTemperaturesByState.csv` | 29 MB |
| `GlobalLandTemperaturesByMajorCity.csv` | 13 MB |
| `GlobalLandTemperaturesByCity.csv` | 508 MB |

```python
# List folder
%fs ls s3://dbx-class-exercise-datasets/climate_change/

# Take a look at the CSV
%fs head s3://dbx-class-exercise-datasets/climate_change/GlobalTemperatures.csv

# Load CSV
df = spark.read.csv("s3://dbx-class-exercise-datasets/climate_change/GlobalTemperatures.csv", <FILL IN CSV READER OPTIONS>)
```

---

## Consumer Reviews (Amazon Products)

Amazon product reviews dataset.

| File | Size |
|------|------|
| `1429_1.csv` | 47 MB |
| `Datafiniti_Amazon_Consumer_Reviews_of_Amazon_Products.csv` | 95 MB |
| `Datafiniti_Amazon_Consumer_Reviews_of_Amazon_Products_May19.csv` | 253 MB |

```python
# List folder
%fs ls s3://dbx-class-exercise-datasets/consumer_reviews_amazon_products/

# Take a look at the CSV
%fs head s3://dbx-class-exercise-datasets/consumer_reviews_amazon_products/1429_1.csv

# Load CSV
df = spark.read.csv("s3://dbx-class-exercise-datasets/consumer_reviews_amazon_products/1429_1.csv", <FILL IN CSV READER OPTIONS>)
```

---

## FIFA

FIFA player data (2015-2022) for male and female players.

| File | Size |
|------|------|
| `players_15.csv` - `players_22.csv` | 10-13 MB each |
| `female_players_16.csv` - `female_players_22.csv` | 140-237 KB each |

```python
# List folder
%fs ls s3://dbx-class-exercise-datasets/fifa/

# Take a look at the CSV
%fs head s3://dbx-class-exercise-datasets/fifa/players_22.csv

# Load CSV
df = spark.read.csv("s3://dbx-class-exercise-datasets/fifa/players_22.csv", <FILL IN CSV READER OPTIONS>)
```

---

## Global Terrorism Database

Terrorism incidents from 1970 onwards with 135 columns of detailed information.

| File | Size |
|------|------|
| `globalterrorismdb_0718dist.csv` | 155 MB |

```python
# List folder
%fs ls s3://dbx-class-exercise-datasets/global_terrorism_database/

# Take a look at the CSV
%fs head s3://dbx-class-exercise-datasets/global_terrorism_database/globalterrorismdb_0718dist.csv

# Load CSV
df = spark.read.csv("s3://dbx-class-exercise-datasets/global_terrorism_database/globalterrorismdb_0718dist.csv", <FILL IN CSV READER OPTIONS>)
```

---

## Goodreads Books

Book metadata including ratings, authors, and publication info (11K+ books).

| File | Size |
|------|------|
| `books.csv` | 1.5 MB |

```python
# List folder
%fs ls s3://dbx-class-exercise-datasets/goodreads_books/

# Take a look at the CSV
%fs head s3://dbx-class-exercise-datasets/goodreads_books/books.csv

# Load CSV
df = spark.read.csv("s3://dbx-class-exercise-datasets/goodreads_books/books.csv", <FILL IN CSV READER OPTIONS>)
```

---

## Powerlifting

OpenPowerlifting competition data with millions of records.

| File | Size |
|------|------|
| `openpowerlifting.csv` | 239 MB |
| `openpowerlifting-2024-01-06-4c732975.csv` | 573 MB |

```python
# List folder
%fs ls s3://dbx-class-exercise-datasets/powerlifting/

# Take a look at the CSV
%fs head s3://dbx-class-exercise-datasets/powerlifting/openpowerlifting.csv

# Load CSV
df = spark.read.csv("s3://dbx-class-exercise-datasets/powerlifting/openpowerlifting.csv", <FILL IN CSV READER OPTIONS>)
```

---

## Rotten Tomatoes Movies & Reviews

Movie metadata and critic reviews.

| File | Size |
|------|------|
| `rotten_tomatoes_movies.csv` | 16 MB |
| `rotten_tomatoes_critic_reviews.csv` | 216 MB |

```python
# List folder
%fs ls s3://dbx-class-exercise-datasets/rotten_tomatoes_movies_reviews/

# Take a look at the CSV
%fs head s3://dbx-class-exercise-datasets/rotten_tomatoes_movies_reviews/rotten_tomatoes_movies.csv

# Load CSV
df = spark.read.csv("s3://dbx-class-exercise-datasets/rotten_tomatoes_movies_reviews/rotten_tomatoes_movies.csv", <FILL IN CSV READER OPTIONS>)
```

---

## San Francisco Building Permits

Building permit records (199K+ permits, 43 columns).

| File | Size |
|------|------|
| `Building_Permits.csv` | 75 MB |

```python
# List folder
%fs ls s3://dbx-class-exercise-datasets/san_francisco_building_permits/

# Take a look at the CSV
%fs head s3://dbx-class-exercise-datasets/san_francisco_building_permits/Building_Permits.csv

# Load CSV
df = spark.read.csv("s3://dbx-class-exercise-datasets/san_francisco_building_permits/Building_Permits.csv", <FILL IN CSV READER OPTIONS>)
```

---

## Used Cars

Used car sales data with pricing, condition, and vehicle details.

| File | Size |
|------|------|
| `car_prices.csv` | 84 MB |

```python
# List folder
%fs ls s3://dbx-class-exercise-datasets/used_cars/

# Take a look at the CSV
%fs head s3://dbx-class-exercise-datasets/used_cars/car_prices.csv

# Load CSV
df = spark.read.csv("s3://dbx-class-exercise-datasets/used_cars/car_prices.csv", <FILL IN CSV READER OPTIONS>)
```

---

## UK Traffic Accidents

UK road accident data (2005-2014) and traffic flow statistics.

| File | Size |
|------|------|
| `accidents_2005_to_2007.csv` | 156 MB |
| `accidents_2009_to_2011.csv` | 129 MB |
| `accidents_2012_to_2014.csv` | 127 MB |
| `ukTrafficAADF.csv` | 53 MB |

```python
# List folder
%fs ls s3://dbx-class-exercise-datasets/uk_traffic_accidents/

# Take a look at the CSV
%fs head s3://dbx-class-exercise-datasets/uk_traffic_accidents/accidents_2012_to_2014.csv

# Load CSV
df = spark.read.csv("s3://dbx-class-exercise-datasets/uk_traffic_accidents/accidents_2012_to_2014.csv", <FILL IN CSV READER OPTIONS>)
```

---

## US Election 2020 Tweets

Twitter data from the 2020 US Presidential Election.

| File | Size |
|------|------|
| `hashtag_donaldtrump.csv` | 461 MB |
| `hashtag_joebiden.csv` | 363 MB |

```python
# List folder
%fs ls s3://dbx-class-exercise-datasets/us_election_2020_tweets/

# Take a look at the CSV
%fs head s3://dbx-class-exercise-datasets/us_election_2020_tweets/hashtag_donaldtrump.csv

# Load CSV
df = spark.read.csv("s3://dbx-class-exercise-datasets/us_election_2020_tweets/hashtag_donaldtrump.csv", <FILL IN CSV READER OPTIONS>)
```

---

## World Cities

Global cities database with population and coordinates (3.2M+ cities).

| File | Size |
|------|------|
| `worldcitiespop.csv` | 157 MB |

```python
# List folder
%fs ls s3://dbx-class-exercise-datasets/world_cities/

# Take a look at the CSV
%fs head s3://dbx-class-exercise-datasets/world_cities/worldcitiespop.csv

# Load CSV
df = spark.read.csv("s3://dbx-class-exercise-datasets/world_cities/worldcitiespop.csv", <FILL IN CSV READER OPTIONS>)
```

---

## Vehicle Sales Data

Car sales data with pricing and vehicle specifications.

| File | Size |
|------|------|
| `car_prices.csv` | 84 MB |

```python
# List folder
%fs ls s3://dbx-class-exercise-datasets/vehicle_sales_data/

# Take a look at the CSV
%fs head s3://dbx-class-exercise-datasets/vehicle_sales_data/car_prices.csv

# Load CSV
df = spark.read.csv("s3://dbx-class-exercise-datasets/vehicle_sales_data/car_prices.csv", <FILL IN CSV READER OPTIONS>)
```

---

## The Movie Database (TMDb)

Movie metadata and credits from TMDb (5000 movies).

| File | Size |
|------|------|
| `tmdb_5000_movies.csv` | 5.4 MB |
| `tmdb_5000_credits.csv` | 38 MB |

```python
# List folder
%fs ls s3://dbx-class-exercise-datasets/the_movie_database/

# Take a look at the CSV
%fs head s3://dbx-class-exercise-datasets/the_movie_database/tmdb_5000_movies.csv

# Load CSV
df = spark.read.csv("s3://dbx-class-exercise-datasets/the_movie_database/tmdb_5000_movies.csv", <FILL IN CSV READER OPTIONS>)
```

---

## Superstore Sales

Retail sales data with orders, returns, and regional managers.

| File | Size |
|------|------|
| `Superstore_Orders.csv` | 2.3 MB |
| `Superstore_Returns.csv` | 5.5 KB |
| `Superstore_People.csv` | 97 B |

```python
# List folder
%fs ls s3://dbx-class-exercise-datasets/superstore_sales/

# Take a look at the CSV
%fs head s3://dbx-class-exercise-datasets/superstore_sales/Superstore_Orders.csv

# Load CSV
df = spark.read.csv("s3://dbx-class-exercise-datasets/superstore_sales/Superstore_Orders.csv", <FILL IN CSV READER OPTIONS>)
```


## Question 1: Understanding Docker Images

**Task:**  
Run Docker with the `python:3.13` image using `bash` as the entrypoint and determine the version of `pip` installed in the image.

**Answer:**  
- **pip version:** `25.3`

---

## Question 2: Understanding Docker Networking and `docker-compose`

**Task:**  
Given a `docker-compose.yaml`, identify the hostname and port that **pgAdmin** should use to connect to the **PostgreSQL** database.

**Answer:**  
- `db:5432`  

---

## Question 3: Counting Short Trips

**Task:**  
For trips in **November 2025** (`lpep_pickup_datetime` between `2025-11-01` and `2025-12-01`, exclusive of the upper bound), count how many trips had a `trip_distance` of **less than or equal to 1 mile**.

**SQL Query:**
```sql
SELECT COUNT(1)
FROM public."green_tripdata_2025-11"
WHERE lpep_pickup_datetime BETWEEN '2025-11-01' AND '2025-12-01'
  AND trip_distance <= 1;
```

**Answer:**  
- `8,007`  

---

## Question 4: Longest Trip for Each Day

**Task:**  
Identify the **pickup day** with the **longest trip distance**, considering only trips with `trip_distance < 100` miles to exclude data errors.  
Use the pickup time for calculations.

**SQL Query:**
```sql
SELECT MAX(trip_distance), lpep_pickup_datetime
FROM public."green_tripdata_2025-11"
WHERE trip_distance <= 100
GROUP BY 2
ORDER BY 1 DESC;
```

**Answer:**  
- `2025-11-14`  

---

## Question 5: Biggest Pickup Zone

**Task:**  
Determine the **pickup zone** with the **largest total_amount** (sum of all trips) on **November 18th, 2025**.

**SQL Query:**
```sql
SELECT tz."Zone", SUM(gt.total_amount)
FROM public."green_tripdata_2025-11" gt
JOIN taxi_zones tz
  ON gt."PULocationID" = tz."LocationID"
WHERE DATE(gt.lpep_pickup_datetime) = '2025-11-18'
GROUP BY 1
ORDER BY 2 DESC;
```

**Answer:**  
- `East Harlem North`  

---

## Question 6: Largest Tip

**Task:**  
For passengers picked up in the zone **"East Harlem North"** during **November 2025**, identify the **drop-off zone** that had the **largest tip**.

> **Note:** This refers to **tip**, not **trip**, and the zone **name** is required (not the ID).

**SQL Query:**
```sql
SELECT
  gt.*,
  ptz."Zone" AS pu_zone,
  dtz."Zone" AS do_zone
FROM public."green_tripdata_2025-11" gt
JOIN taxi_zones ptz
  ON gt."PULocationID" = ptz."LocationID"
JOIN taxi_zones dtz
  ON gt."DOLocationID" = dtz."LocationID"
WHERE EXTRACT(MONTH FROM lpep_pickup_datetime) = 11
  AND ptz."Zone" = 'East Harlem North'
ORDER BY gt.tip_amount DESC;
```

**Answer:**  
- `Yorkville West`  

---

## Question 7: Terraform Workflow

**Task:**  
Identify the correct sequence of Terraform commands for:

1. Downloading provider plugins and setting up the backend  
2. Generating and automatically applying the execution plan  
3. Removing all resources managed by Terraform  

**Answer:**
```text
terraform init
terraform apply -auto-approve
terraform destroy
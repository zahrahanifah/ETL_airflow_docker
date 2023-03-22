#Below are fact table
create_table_fact_review = '''
CREATE TABLE IF NOT EXISTS fact_review (
    review_id varchar(256), 
    user_id varchar(256), 
    business_id varchar(256), 
    stars int4, 
    useful int4, 
    funny int4, 
    cool int4, 
    text varchar(5000), 
    date date)
'''

fact_review_insert = '''
        INSERT INTO fact_review (date,review_id,user_id,business_id,stars, useful, funny, cool, text)
        SELECT stg_review.date::date,
               review_id,
               user_id,
               business_id,
               stars, 
               useful, 
               funny, 
               cool, 
               text
            FROM stg_review 
    '''

create_table_fact_tip = '''
CREATE TABLE IF NOT EXISTS fact_tip (
    user_id varchar(256), 
    business_id varchar(256), 
    text varchar(5000), 
    date date, 
    compliment_count int4)
'''

fact_tip_insert = '''
        INSERT INTO fact_tip (date, user_id, business_id, text, compliment_count)
        SELECT stg_tip.date::date,
               user_id,
               business_id,
               text,
               compliment_count
            FROM stg_tip
    '''

#Below are dim table
create_table_dim_business = '''
CREATE TABLE IF NOT EXISTS dim_business (
    business_id varchar(256), 
    name varchar(256), 
    address varchar(1000), 
    city varchar(256), 
    state varchar(256), 
    postal_code varchar(256), 
    latitude varchar(256), 
    longitude varchar(256), 
    stars int,
    review_count int,
    is_open int,
    attributes varchar(1000),
    categories varchar(1000),
    hours varchar(1000))
'''

dim_business_insert = '''
        INSERT INTO dim_business (business_id, name, address, city, state, postal_code, latitude, longitude, stars,review_count,is_open,attributes,categories,hours)
        SELECT business_id, 
               name, 
               address, 
               city, 
               state, 
               postal_code, 
               latitude, 
               longitude, 
               stars,
               review_count,
               is_open,
               attributes,
               categories,
               hours
               FROM stg_business
    '''

create_table_dim_user = '''
CREATE TABLE IF NOT EXISTS dim_user (
    user_id varchar(256), 
    name varchar(256), 
    review_count int, 
    yelping_since date, 
    useful int, 
    funny int, 
    cool int, 
    elite varchar(1000), 
    friends varchar)
'''

dim_user_insert = '''
        INSERT INTO dim_user (user_id, name, review_count, yelping_since, useful, funny, cool, elite, friends)
        SELECT user_id, 
               name, 
               review_count, 
               yelping_since::date, 
               useful, 
               funny, 
               cool, 
               elite, 
               friends
               FROM stg_user 
    '''


#Below are serving table

create_table_srv_review = '''
CREATE TABLE IF NOT EXISTS srv_review  (
    review_id varchar(256), 
    user_id varchar(256), 
    business_id varchar(256), 
    stars int4, 
    useful int4, 
    funny int4, 
    cool int4, 
    text varchar(5000), 
    date date, 
    min int4, 
    max int4, 
    normal_min int4, 
    normal_max int4,
    precipitation float8,
    precipitation_normal float8)
'''

srv_review_insert = '''
        INSERT INTO srv_review  (date,review_id,user_id,business_id,stars, useful, funny, cool, text, min, max, normal_min, normal_max,precipitation,precipitation_normal)
        SELECT stg_review.date::date,
               review_id,
               user_id,
               business_id,
               stars, 
               useful, 
               funny, 
               cool, 
               text,
               min,
               max,
               normal_min,
               normal_max,
               precipitation,
               precipitation_normal
            FROM stg_review 
            LEFT JOIN (SELECT date::date,
                              min,
                              max,
                              normal_min,
                              normal_max
                       FROM stg_temperature) temperature
            ON DATE(stg_review.date) = DATE(temperature.date)
            LEFT JOIN (SELECT date::date,
                              precipitation::float,
                              precipitation_normal
                       FROM stg_precipitation) stg_precipitation
            ON DATE(stg_review.date) = DATE(stg_precipitation.date)
    '''

create_table_srv_tip = '''
CREATE TABLE IF NOT EXISTS srv_tip (
    user_id varchar(256), 
    business_id varchar(256), 
    text varchar(5000), 
    date date, 
    compliment_count int4,
    min int4, 
    max int4, 
    normal_min int4, 
    normal_max int4,
    precipitation float8,
    precipitation_normal float8)
'''

srv_tip_insert = '''
        INSERT INTO srv_tip (date, user_id, business_id, text, compliment_count, min, max, normal_min, normal_max,precipitation,precipitation_normal)
        SELECT stg_tip.date::date,
               user_id,
               business_id,
               text,
               compliment_count,
               min,
               max,
               normal_min,
               normal_max,
               precipitation,
               precipitation_normal
            FROM stg_tip
            LEFT JOIN (SELECT date::date,
                              min,
                              max,
                              normal_min,
                              normal_max
                       FROM stg_temperature) temperature
            ON DATE(stg_tip.date) = DATE(temperature.date)
            LEFT JOIN (SELECT date::date,
                              precipitation::float,
                              precipitation_normal
                       FROM stg_precipitation) stg_precipitation
            ON DATE(stg_tip.date) = DATE(stg_precipitation.date)
    '''


#Below are aggregate table
create_table_AggReviewDay = '''
CREATE TABLE IF NOT EXISTS agg_ReviewDay (
    date date, 
    stars char,
    count_review int4,
    min int4,
    max int4,
    normal_min int4, 
    normal_max int4,
    precipitation float8,
    precipitation_normal float8)
'''

AggReviewDay_insert = '''
        INSERT INTO agg_ReviewDay(date, stars, count_review, min, max, normal_min, normal_max,precipitation,precipitation_normal)
        SELECT review.date::date, 
               review.stars::char,
               count_review, 
               min, 
               max, 
               normal_min, 
               normal_max,
               precipitation::float,
               precipitation_normal
        FROM (SELECT fact_review.date::date,
                     fact_review.stars::char,
                     COUNT(review_id) AS count_review
              FROM fact_review 
              GROUP BY fact_review.date, fact_review.stars::char) AS review
        LEFT JOIN stg_temperature
        ON DATE(review.date) = DATE(stg_temperature.date)
        LEFT JOIN stg_precipitation
        ON DATE(review.date) = DATE(stg_precipitation.date);
    '''


#Below are aggregate table
create_table_AggTipDay = '''
CREATE TABLE IF NOT EXISTS Agg_TipDay (
    date date, 
    count_tip int4,
    min int4,
    max int4,
    normal_min int4, 
    normal_max int4,
    precipitation float8,
    precipitation_normal float8)
'''

AggTipDay_insert = '''
        INSERT INTO Agg_TipDay(date, count_tip, min, max, normal_min, normal_max,precipitation,precipitation_normal)
        SELECT tip.date::date, 
               COUNT_tip, 
               min, 
               max, 
               normal_min, 
               normal_max,
               precipitation::float,
               precipitation_normal
        FROM (SELECT fact_tip.date::date,
                     COUNT(user_id) AS count_tip
              FROM fact_tip 
              GROUP BY fact_tip.date) AS tip
        LEFT JOIN stg_temperature
        ON DATE(tip.date) = DATE(stg_temperature.date)
        LEFT JOIN stg_precipitation
        ON DATE(tip.date) = DATE(stg_precipitation.date);
    '''
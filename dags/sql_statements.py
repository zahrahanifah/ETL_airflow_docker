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
    address varchar(256), 
    city varchar(256), 
    state varchar(256), 
    postal_code varchar(256), 
    latitude varchar(256), 
    longitude varchar(256), 
    starts int5,
    review_count int7,
    is_open int4,
    attributes varchar(256),
    categories varchar(256),
    hours varchar(256))
'''

dim_business_insert = '''
        INSERT INTO dim_business (business_id, name, address, city, state, postal_code, latitude, longitude, starts,review_count,is_open,attributes,categories ,hours)
        SELECT business_id, 
               name, 
               address, 
               city, 
               state, 
               postal_code, 
               latitude, 
               longitude, 
               starts,
               review_count,
               is_open,
               attributes,
               categories ,
               hours
               FROM stg_business
    '''

create_table_user_business = '''
CREATE TABLE IF NOT EXISTS dim_user (
    user_id varchar(256), 
    name varchar(256), 
    review_count int, 
    yelping_since date, 
    useful int, 
    funny int, 
    cool int, 
    elite varchar(256), 
    friends varchar(5000))
'''

dim_user_insert = '''
        INSERT INTO dim_user (user_id, name, review_count, yelping_since, useful, funny, cool, elite, friends)
        SELECT user_id, 
               name, 
               review_count, 
               yelping_since, 
               useful, 
               funny, 
               cool, 
               elite, 
               friends
               FROM stg_user 
    '''


#Below are serving table

create_table_srv_review = '''
CREATE TABLE IF NOT EXISTS fact_review (
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
    normal_max int4)
'''

srv_review_insert = '''
        INSERT INTO fact_review (date,review_id,user_id,business_id,stars, useful, funny, cool, text, min, max, normal_min, normal_max)
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
               normal_max
            FROM stg_review 
            LEFT JOIN (SELECT date::date,
                              min,
                              max,
                              normal_min,
                              normal_max
                       FROM stg_temperature) temperature
            ON DATE(stg_review.date) = DATE(temperature.date)
    '''

create_table_srv_tip = '''
CREATE TABLE IF NOT EXISTS fact_tip (
    user_id varchar(256), 
    business_id varchar(256), 
    text varchar(5000), 
    date date, 
    compliment_count int4,
    min int4, 
    max int4, 
    normal_min int4, 
    normal_max int4)
'''

srv_tip_insert = '''
        INSERT INTO fact_tip (date, user_id, business_id, text, compliment_count, min, max, normal_min, normal_max)
        SELECT stg_tip.date::date,
               user_id,
               business_id,
               text,
               compliment_count,
               min,
               max,
               normal_min,
               normal_max
            FROM stg_tip
            LEFT JOIN (SELECT date::date,
                              min,
                              max,
                              normal_min,
                              normal_max
                       FROM stg_temperature) temperature
            ON DATE(stg_tip.date) = DATE(temperature.date)
    '''


DROP TABLE IF EXISTS business;
CREATE TABLE business (
    business_id CHAR(22) PRIMARY KEY,
    name VARCHAR(255),
    address TEXT,
    city varchar(100),
    state VARCHAR(5),
    postal_code VARCHAR(7),
    latitude FLOAT,
    longitude FLOAT,
    stars FLOAT,
    review_count INTEGER,
    is_open INTEGER,
    attributes SUPER,
    categories varchar(500),
    hours SUPER
);

COPY business FROM 's3://c732-hpa61-a5-haomingpan/ProjectYelp/business/'
IAM_ROLE 'arn:aws:iam::[Your_IAM_id]:role/RedshiftS3Full'
FORMAT JSON 'auto';

DROP TABLE IF EXISTS users;
CREATE TABLE users (
    user_id CHAR(22) PRIMARY KEY,
    name VARCHAR(100),
    review_count FLOAT,
    yelping_since DATETIME,
    useful FLOAT,
    funny FLOAT,
    cool FLOAT,
    elite VARCHAR(255),
    fans FLOAT,
    average_stars FLOAT, 
    num_elite FLOAT
);
COPY users FROM 's3://c732-hpa61-a5-haomingpan/ProjectYelp/users/'
IAM_ROLE 'arn:aws:iam::[Your_IAM_id]:role/RedshiftS3Full'
FORMAT JSON 'auto';

DROP TABLE IF EXISTS reviews;
CREATE TABLE reviews (
    review_id CHAR(22) PRIMARY KEY,
    user_id CHAR(22) REFERENCES users(user_id),
    business_id CHAR(22) REFERENCES business(business_id),
    stars FLOAT,
    useful FLOAT,
    funny FLOAT,
    cool FLOAT,
    date DATETIME
);

COPY reviews FROM 's3://c732-hpa61-a5-haomingpan/ProjectYelp/yelp_academic_dataset_review.json'
IAM_ROLE 'arn:aws:iam::[Your_IAM_id]:role/RedshiftS3Full'
FORMAT JSON 'auto';

DROP TABLE IF EXISTS tips;
CREATE TABLE tips (
  tip_id bigint identity(1, 1),
  user_id CHAR(22) REFERENCES users(user_id),
  business_id CHAR(22) REFERENCES business(business_id),
  date DATETIME,
  compliment_count INT 
);

COPY tips FROM 's3://c732-hpa61-a5-haomingpan/ProjectYelp/yelp_academic_dataset_tip.json'
IAM_ROLE 'arn:aws:iam::[Your_IAM_id]:role/RedshiftS3Full'
FORMAT JSON 'auto';




REATE TABLE high_rate_reataurant_2008 (
  name VARCHAR(255) PRIMARY KEY,
  order_numbers int,
  average_stars float,
  userful_nums int
);

COPY high_rate_reataurant_2008 FROM 's3://c732-hpa61-a5-haomingpan/ProjectYelp/High_rate_restaurant/2008'
IAM_ROLE 'arn:aws:iam::xxxxxxxxx:role/RedshiftS3Full'
FORMAT JSON 'auto';



CREATE TABLE high_rate_reataurant_2012 (
  name VARCHAR(255) PRIMARY KEY,
  order_numbers int,
  average_stars float,
  userful_nums int
);

COPY high_rate_reataurant_2012 FROM 's3://c732-hpa61-a5-haomingpan/ProjectYelp/High_rate_restaurant/2012'
IAM_ROLE 'arn:aws:iam::xxxxxxxxx:role/RedshiftS3Full'
FORMAT JSON 'auto';



CREATE TABLE high_rate_reataurant_2016 (
  name VARCHAR(255) PRIMARY KEY,
  order_numbers int,
  average_stars float,
  userful_nums int
);

COPY high_rate_reataurant_2016 FROM 's3://c732-hpa61-a5-haomingpan/ProjectYelp/High_rate_restaurant/2016'
IAM_ROLE 'arn:aws:iam::xxxxxxxxx:role/RedshiftS3Full'
FORMAT JSON 'auto';




CREATE TABLE high_rate_reataurant_2020 (
  name VARCHAR(255) PRIMARY KEY,
  order_numbers int,
  average_stars float,
  userful_nums int
);

COPY high_rate_reataurant_2020 FROM 's3://c732-hpa61-a5-haomingpan/ProjectYelp/High_rate_restaurant/2020'
IAM_ROLE 'arn:aws:iam::xxxxxxxxx:role/RedshiftS3Full'
FORMAT JSON 'auto';



# Select all the resaurants survived from 2008 to 2020 in superset

SELECT t1.name
FROM high_rate_reataurant_2008 t1, high_rate_reataurant_2012 t2, high_rate_reataurant_2016 t3, high_rate_reataurant_2020 t4
WHERE t1.name = t2.name and t2.name = t3.name and t3.name = t4.name


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

DROP TABLE IF EXISTS dogsallowed_stars;
CREATE TABLE dogsallowed_stars(
	dogsallowed VARCHAR(20),
	stars FLOAT
);
COPY dogsallowed_stars FROM 's3://c732-hpa61-a5-haomingpan/ProjectYelp/attributes/dogsallowed_stars/'
IAM_ROLE 'arn:aws:iam::[Your_IAM_id]:role/RedshiftS3Full'
FORMAT JSON 'auto';

CREATE TABLE noiselevel_stars(
	noiselevel VARCHAR(20),
	stars FLOAT
);
COPY noiselevel_stars FROM 's3://c732-hpa61-a5-haomingpan/ProjectYelp/attributes/noiselevel_stars/'
IAM_ROLE 'arn:aws:iam::[Your_IAM_id]:role/RedshiftS3Full'
FORMAT JSON 'auto';

CREATE TABLE wifi_stars(
	wifi VARCHAR(20),
	stars FLOAT
);
COPY wifi_stars FROM 's3://c732-hpa61-a5-haomingpan/ProjectYelp/attributes/wifi_stars/'
IAM_ROLE 'arn:aws:iam::[Your_IAM_id]:role/RedshiftS3Full'
FORMAT JSON 'auto';

CREATE TABLE good_review_business(
	business_id CHAR(22),
	good_review INTEGER,
	total_review INTEGER,
	name VARCHAR(255),
	state VARCHAR(5),
	good_review_ratio FLOAT
);
COPY good_review_business FROM 's3://c732-hpa61-a5-haomingpan/ProjectYelp/reviews_goodness/good_review_business/'
IAM_ROLE 'arn:aws:iam::[Your_IAM_id]:role/RedshiftS3Full'
FORMAT JSON 'auto';

CREATE TABLE bad_review_business(
	business_id CHAR(22),
	bad_review INTEGER,
	total_review INTEGER,
	name VARCHAR(255),
	state VARCHAR(5),
	bad_review_ratio FLOAT
);
COPY bad_review_business FROM 's3://c732-hpa61-a5-haomingpan/ProjectYelp/reviews_goodness/bad_review_business/'
IAM_ROLE 'arn:aws:iam::[Your_IAM_id]:role/RedshiftS3Full'
FORMAT JSON 'auto';


CREATE TABLE reviews_states(
	state VARCHAR(5),
	review_number INTEGER
);
COPY reviews_states FROM 's3://c732-hpa61-a5-haomingpan/ProjectYelp/reviews_states/reviews_states/'
IAM_ROLE 'arn:aws:iam::[Your_IAM_id]:role/RedshiftS3Full'
FORMAT JSON 'auto';


-- 1. Import data to database
CREATE OR REPLACE FUNCTION import_csv_auto_table(
    -- path to import CSV file
    p_file_path TEXT,
    p_table_name TEXT DEFAULT NULL,
    p_delimiter TEXT DEFAULT ',',
    p_drop_if_exists BOOLEAN DEFAULT FALSE
)
    RETURNS TEXT AS
$$
DECLARE
    v_table_name  TEXT;
    v_header_line TEXT;
    v_headers     TEXT[];
    v_create_sql  TEXT;
BEGIN
    -- Determine the table name
    -- Use the provider name or the filename
    IF p_table_name IS NULL THEN
        v_table_name := regexp_replace(p_file_path, '^.*[/\\]([^/\\]+)$', '\1');
        v_table_name := regexp_replace(v_table_name, '\.[^.]*$', '');
        v_table_name := regexp_replace(v_table_name, '[^a-zA-Z0-9_]', '_', 'g');
        v_table_name := regexp_replace(v_table_name, '^([0-9])', '_\1');
    ELSE
        v_table_name := p_table_name;
    END IF;

    -- Drop table if it exists
    IF p_drop_if_exists THEN
        EXECUTE format('DROP TABLE IF EXISTS %I', v_table_name);
    END IF;

    -- Create TEMP table
    CREATE TEMP TABLE temp_header_line
    (
        line TEXT
    );
    EXECUTE format('COPY temp_header_line FROM %L WITH (FORMAT TEXT)', p_file_path);
    SELECT line INTO v_header_line FROM temp_header_line LIMIT 1;
    DROP TABLE temp_header_line;

    -- Create table
    v_headers := string_to_array(v_header_line, p_delimiter);
    v_create_sql := 'CREATE TABLE ' || quote_ident(v_table_name) || ' ( ';
    FOR i IN 1..array_length(v_headers, 1)
        LOOP
            v_create_sql := v_create_sql || quote_ident(trim(both '"' from v_headers[i])) || ' TEXT';
            IF i < array_length(v_headers, 1) THEN
                v_create_sql := v_create_sql || ', ';
            END IF;
        END LOOP;
    v_create_sql := v_create_sql || ')';

    EXECUTE v_create_sql;
    EXECUTE format('COPY %I FROM %L WITH (FORMAT CSV, DELIMITER %L, HEADER TRUE)',
                   v_table_name, p_file_path, p_delimiter);
    RETURN format('Table "%s" created and data import successfully.', v_table_name);

EXCEPTION
    WHEN OTHERS THEN
        BEGIN
            EXECUTE format('DROP TABLE IF EXISTS %I', v_table_name);
        EXCEPTION
            WHEN OTHERS THEN NULL;
        END;
        RAISE EXCEPTION 'Error during CSV import: %', SQLERRM;
END;
$$ LANGUAGE plpgsql;

-- allow only files
SELECT import_csv_auto_table('C:\Users\ADMIN\Desktop\SampleCSV\customer1.csv');


-- 2. Create upsert function

CREATE OR REPLACE FUNCTION upsert_fn(
    p_schema_name TEXT,
    p_table_name TEXT,
    p_key_column TEXT,
    p_csv_path TEXT
)
    RETURNS VOID AS
$$
DECLARE
    v_temp_table_name TEXT := 'temp_upsert_' || floor(random() * 1000000)::TEXT;
    v_all_columns     TEXT;
    v_update_columns  TEXT;
    v_sql_statement   TEXT;
BEGIN
    -- Create a temporary table with the exact structure of the target table
    v_sql_statement := format(
            'CREATE TEMP TABLE %I (LIKE %I.%I INCLUDING ALL)',
            v_temp_table_name, p_schema_name, p_table_name
    );
    EXECUTE v_sql_statement;

    -- Load data from the CSV file into the temporary table.
    v_sql_statement := format(
            'COPY %I FROM %L WITH (FORMAT csv, HEADER true)',
            v_temp_table_name, p_csv_path
    );
    EXECUTE v_sql_statement;

    -- Dynamically get a list of all column names for the INSERT part
    SELECT string_agg(quote_ident(column_name), ', ')
    INTO v_all_columns
    FROM information_schema.columns
    WHERE table_schema = p_schema_name
      AND table_name = p_table_name;

    -- Dynamically build a list of columns to update, excluding the key column
    SELECT string_agg(format('%I = EXCLUDED.%I', column_name, column_name), ', ')
    INTO v_update_columns
    FROM information_schema.columns
    WHERE table_schema = p_schema_name
      AND table_name = p_table_name
      AND column_name <> p_key_column;

    -- Construct the final UPSERT command and execute it
    v_sql_statement := format(
            'INSERT INTO %I.%I (%s)
            SELECT %s FROM %I
            ON CONFLICT (%I) DO UPDATE SET %s',
            p_schema_name, p_table_name,
            v_all_columns, v_all_columns, v_temp_table_name,
            p_key_column, v_update_columns
    );
    EXECUTE v_sql_statement;

    -- Clean up by dropping the temporary table
    v_sql_statement := format('DROP TABLE IF EXISTS %I', v_temp_table_name);
    EXECUTE v_sql_statement;
END;
$$ LANGUAGE plpgsql;

-- add primary key for unique key
ALTER TABLE public.customer1
ADD CONSTRAINT customer1_customer_id_unique UNIQUE (customer_id);

-- use the upsert fn
SELECT upsert_fn(
               'public',
               'customer1',
               'customer_id',
               'C:/Users/ADMIN/Desktop/SampleCSV/customer1.csv'
);


-- 3. see the logs of customer changes

-- table store event logs
CREATE TABLE IF NOT EXISTS customer_audit_log(
    log_id SERIAL PRIMARY KEY,
    customer_id INT NOT NULL,
    operation_type VARCHAR(30),
    old_data JSONB,
    new_data JSONB,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- function to logs the process when event happen
CREATE OR REPLACE FUNCTION log_customer_changes()
RETURNS TRIGGER AS $$
BEGIN
    -- events INSERT
    IF (TG_OP = 'INSERT') THEN
        INSERT INTO customer_audit_log(customer_id, operation_type, new_data)
        VALUES (NEW.customer_id::INTEGER, TG_OP::TEXT, to_jsonb(NEW));
        RETURN NEW;
    -- events UPDATE
    ELSIF (TG_OP = 'UPDATE') THEN
        -- log the old data and update
        INSERT INTO customer_audit_log(customer_id, operation_type, old_data, new_data)
        VALUES (OLD.customer_id::INTEGER, TG_OP::TEXT, to_jsonb(OLD), to_jsonb(NEW));
        RETURN NEW;
    -- events DELETE
    ELSIF (TG_OP = 'DELETE') THEN
        INSERT INTO customer_audit_log(customer_id,operation_type,old_data)
        VALUES (OLD.customer_id::INTEGER, TG_OP::TEXT, to_jsonb(OLD));
        RETURN OLD;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Insert to log the process if duplicate do nothing
INSERT INTO customer1(customer_id, customer_name, gender, segment, country, city, state, age)
VALUES (17,'Jane','M','Consumer','United State','Canada','CN',19);


-- EXECUTE when event happen
CREATE OR REPLACE TRIGGER customer_modification_trigger
AFTER INSERT OR UPDATE OR DELETE ON customer1
FOR EACH ROW
EXECUTE FUNCTION log_customer_changes();


-- 4. Normalize

-- This is sample data from movies table
-- movie_id	        title	                year	rating	        runtime	        genre	                        released	            director
-- 1	                Pauvre Pierrot	        1892		            4 min	        Animation, Comedy,Short	        1892-10-28	            mile Reynaud
-- 2	                Blacksmith Scene	    1893	UNRATED	        1 min	        Short	                        1893-05-09	            William K.L. Dickson
-- 3	                Edison Kinetoscopic 	1894		            1 min	        Documentary, Short	            1894-01-09	            William K.L. Dickson
-- 4	                Tables Turned       	1895		            1 min	        Comedy, Short		                                    Louis Lumre
-- 5	                Baby's Dinner	        1895		            1 min	        Documentary, Short	            1895-12-28	            Louis Lumre

-- This table isn't 1NF
-- Have multiple values in a column



                                                -- 1NF

-- we need to split multiple values into a separate rows
-- every cell is must have atomic values

-- movies
-- movie_id	            title	              year	            rating	        runtime	            genre	                released	                director
-- 1	            Pauvre Pierrot	          1892		                        4 min	            Animation	            1892-10-28	                mile Reynaud
-- 1	            Pauvre Pierrot	          1892		                        4 min	            Comedy	                1892-10-28	                mile Reynaud
-- 1	            Pauvre Pierrot	          1892		                        4 min	            Short	                1892-10-28	                mile Reynaud
-- 2	            Blacksmith Scene	      1893	            UNRATED	        1 min	            Short	                1893-05-09	                William K.L. Dickson
-- 3	            Edison Kinetoscopic 	  1894		                        1 min	            Documentary	            1894-01-09	                William K.L. Dickson
-- 3	            Edison Kinetoscopic 	  1894		                        1 min	            Short	                1894-01-09	                William K.L. Dickson
-- 4	            Tables Turned 	          1895		                        1 min	            Comedy		                                        Louis Lumre
-- 4	            Tables Turned 	          1895		                        1 min	            Short		                                        Louis Lumre
-- 5	            Baby's Dinner	          1895		                        1 min	            Documentary	            1895-12-28	                Louis Lumre
-- 5	            Baby's Dinner	          1895		                        1 min	            Short	                1895-12-28	                Louis Lumre




                                                -- 2NF

-- we already have an 1NF
-- all non-key attributes must depend on the hold key we have like primary key
-- relationship
    -- one 'movie' can have multiple 'genres' or one genre can belong to multiple movies,
            -- so we need to create the junction table to store this relationship together
    -- one 'movie' can have multiple directors or one director can direct multiple movies
            -- so we need to create the junction table to store this relationship together

-- movies --
-- movie_id	    title	                year	        rating	        runtime                             released
-- 1	        Pauvre Pierrot	        1892		                    4 min	                            1892-10-28
-- 2	        Blacksmith Scene	    1893	        UNRATED	        1 min	                            1893-05-09
-- 3	        Edison Kinetoscopic     1894		                    1 min	                            1894-01-09
-- 4	        Tables Turned	        1895		                    1 min
-- 5	        Baby's Dinner	        1895		                    1 min	                            1895-12-28


-- genres
-- genre_id    name
-- 1           Animation
-- 2           Comedy
-- 3           Short
-- 4           Documentary


-- directors
-- director_id         name
-- 1                   mile Reynaud
-- 2                   William K.L. Dickson
-- 3                   Louis Lumre


-- movie_genres
-- movie_id            genre_id
-- 1                   1
-- 1                   2
-- 1                   3
-- 2                   3
-- 3                   4
-- 3                   3
-- 4                   2
-- 4                   3
-- 5                   3


-- movie_directors
-- movie_id                director_id
-- 1                       1
-- 1                       2
-- 3                       2
-- 4                       3
-- 5                       3

                                                -- 3NF

-- our data are in 2NF so we can do 3NF isn't. can't do it
-- we need to remove the transitive dependencies non-key attributes must depend only on the primary key

-- movies
-- movie_id	    title	              year              rating	        runtime                          released
-- 1	    Pauvre Pierrot	          1892                                4 min	                         1892-10-28
-- 2	    Blacksmith Scene          1893              UNRATED	          1 min	                         1893-05-09
-- 3	    Edison Kinetoscopic       1894                                1 min	                         1894-01-09
-- 4	    Tables Turned	          1895                                1 min
-- 5	    Baby's Dinner	          1895                                1 min	                         1895-12-28


-- genres
-- genre_id    name
-- 1           Animation
-- 2           Comedy
-- 3           Short
-- 4           Documentary

-- directors
-- director_id         name
-- 1                   mile Reynaud
-- 2                   William K.L. Dickson
-- 3                   Louis Lumre


-- movie_genres
-- movie_id            genre_id
-- 1                   1
-- 1                   2
-- 1                   3
-- 2                   3
-- 3                   4
-- 3                   3
-- 4                   2
-- 4                   3
-- 5                   3

-- movie_directors
-- movie_id                director_id
-- 1                       1
-- 2                       2
-- 3                       2
-- 4                       3
-- 5                       3


-- create the rentals table to store data of rentals systems

-- rentals
-- rental_id               customer_id                 movie_id                rental_date             return_date
-- 1                          1                           1                    20-08-2025              25-08-2025


-- script create tables

CREATE TABLE customers (
    customer_id      SERIAL PRIMARY KEY,
    customer_name    VARCHAR(100) NOT NULL,
    gender           CHAR(1),
    age              INT,
    phone            VARCHAR(20),
    email            VARCHAR(100)
);


CREATE TABLE movies(
    movie_id SERIAL PRIMARY KEY,
    title TEXT,
    year INT,
    rating VARCHAR(50),
    runtime_minutes VARCHAR(40),
    release_date DATE
);

CREATE TABLE genres(
    genre_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);

CREATE TABLE directors(
    director_id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE movie_genres(
    movie_id INT NOT NULL,
    genre_id INT NOT NULL,
    PRIMARY KEY (movie_id,genre_id),
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id) ON DELETE CASCADE,
    FOREIGN KEY (genre_id) REFERENCES genres(genre_id) ON DELETE CASCADE
);

CREATE TABLE movie_directors(
    movie_id INT NOT NULL,
    director_id INT NOT NULL,
    PRIMARY KEY (movie_id,director_id),
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id) ON DELETE CASCADE,
    FOREIGN KEY (director_id) REFERENCES directors(director_id) ON DELETE CASCADE
);

CREATE TABLE rentals (
    rental_id        SERIAL PRIMARY KEY,
    customer_id      INT REFERENCES customers(customer_id),
    movie_id         INT REFERENCES movies(movie_id),
    rental_date      DATE NOT NULL,
    return_date      DATE
);


-- Step 1: Create temporary staging table matching your CSV structure
CREATE TEMP TABLE temp_movies_raw (
    movie_id TEXT,
    title TEXT,
    year TEXT,
    rating TEXT,
    runtime TEXT,
    genre TEXT,
    released TEXT,
    director TEXT,
    imdbrating TEXT,
    imdbvotes TEXT,
    plot TEXT,
    fullplot TEXT,
    language TEXT,
    country TEXT,
    awards TEXT,
    lastupdated TEXT,
    type TEXT
);

-- Step 2: Load CSV data into staging table
-- Replace 'path/to/your/movies.csv' with your actual CSV file path
COPY temp_movies_raw
FROM 'C:\Users\ADMIN\Desktop\SampleCSV\movies.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',');


SELECT * FROM temp_movies_raw;

-- get data from temp table and insert to directors
INSERT INTO directors (name)
SELECT DISTINCT TRIM(director_name)
FROM (
    SELECT unnest(string_to_array(director, ',')) as director_name
    FROM temp_movies_raw
    WHERE director IS NOT NULL
    AND director != ''
    AND director != 'N/A'
) AS director_list
WHERE TRIM(director_name) != ''
AND TRIM(director_name) != 'N/A'
ON CONFLICT DO NOTHING;


-- get data from temp table and insert to genres
INSERT INTO genres (name)
SELECT DISTINCT TRIM(unnested_genre)
FROM temp_movies_raw,
     LATERAL regexp_split_to_table(genre, ',') AS unnested_genre
WHERE genre IS NOT NULL;


-- get data from temp table and insert to movie
INSERT INTO movies(movie_id, title, year, rating,runtime_minutes, release_date)
SELECT DISTINCT movie_id::INTEGER, title, year::INTEGER, rating, runtime, released::DATE
FROM temp_movies_raw
WHERE title IS NOT NULL
ON CONFLICT(movie_id) DO NOTHING;


-- get data from temp table and insert to movie_directors
INSERT INTO movie_directors (movie_id, director_id)
SELECT
    trm.movie_id::INTEGER,
    d.director_id
FROM
    temp_movies_raw AS trm
JOIN
    directors AS d ON trm.director = d.name
ON CONFLICT (movie_id, director_id) DO NOTHING;


-- get data from temp table and insert to movie_genre
INSERT INTO movie_genres (movie_id, genre_id)
SELECT
    trm.movie_id::INTEGER,
    g.genre_id
FROM
    temp_movies_raw AS trm,
    LATERAL regexp_split_to_table(trm.genre, ',') AS unnested_genre
JOIN
    genres AS g ON TRIM(unnested_genre) = g.name
ON CONFLICT (movie_id, genre_id) DO NOTHING;

-- Clean up the temporary table.
DROP TABLE temp_movies_raw;


-- INSERT data into customers
INSERT INTO customers (customer_name, gender, age, phone, email) VALUES
('Sok Dara', 'M', 28, '012345678', 'sokdara@example.com'),
('Chan Sreyneang', 'F', 32, '098765432', 'sreyneang.chan@example.com'),
('Ly Vannak', 'M', 45, '097123456', 'ly.vannak@example.com'),
('Kim Sophea', 'F', 26, '092876543', 'kim.sophea@example.com'),
('Chhun Ratha', 'M', 38, '010234567', 'chhun.ratha@example.com'),
('Men Pisey', 'F', 22, '011987654', 'men.pisey@example.com'),
('Phan Sokchea', 'M', 30, '093456789', 'phan.sokchea@example.com'),
('Touch Socheat', 'F', 40, '015123789', 'touch.socheat@example.com'),
('Khun Borey', 'M', 35, '096543210', 'khun.borey@example.com'),
('Ros Sreypov', 'F', 29, '088765432', 'ros.sreypov@example.com');


-- INSERT data into rentals
INSERT INTO rentals (customer_id, movie_id, rental_date, return_date) VALUES
(1, 1, '2025-08-01', '2025-08-05'),
(2, 3, '2025-08-02', '2025-08-06'),
(3, 5, '2025-08-03', '2025-08-08'),
(4, 2, '2025-08-04', '2025-08-07'),
(5, 4, '2025-08-05', '2025-08-10'),
(6, 6, '2025-08-06', '2025-08-11'),
(7, 7, '2025-08-07', '2025-08-12'),
(8, 8, '2025-08-08', '2025-08-15'),
(9, 9, '2025-08-09', '2025-08-14'),
(10, 10, '2025-08-10', '2025-08-16'),
(1, 2, '2025-08-11', '2025-08-14'),
(3, 4, '2025-08-12', '2025-08-17'),
(5, 1, '2025-08-13', '2025-08-18'),
(7, 5, '2025-08-14', '2025-08-19'),
(9, 3, '2025-08-15', '2025-08-20');



-- comprehensive view
DROP VIEW customer_view_rentals;

CREATE OR REPLACE VIEW customer_view_rentals AS
WITH movie_info AS (
    SELECT
        m.title AS movie_title,
        m.release_date AS release_date,
        d.name AS director_name,
        c.customer_name,
        c.age,
        STRING_AGG(g.name,', ') AS genre_names,
        r.rental_date,
        r.return_date
    FROM movies m
    INNER JOIN movie_directors md on m.movie_id = md.movie_id
    INNER JOIN directors d on d.director_id = md.director_id
    INNER JOIN rentals r on m.movie_id = r.movie_id
    INNER JOIN customers c on c.customer_id = r.customer_id
    INNER JOIN movie_genres mg on m.movie_id = mg.movie_id
    INNER JOIN genres g on mg.genre_id = g.genre_id
    GROUP BY m.movie_id, m.title, m.release_date, d.name, c.customer_name, c.age, r.rental_date, r.return_date
)
SELECT * FROM movie_info;

-- select to see the process
SELECT * FROM customer_view_rentals;


-- Script export to csv
copy (SELECT * FROM customer_view_rentals)
TO 'C:\Users\ADMIN\Desktop\SampleCSV/customer_rentals.csv'
WITH CSV HEADER;


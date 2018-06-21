CREATE DATABASE MainDB;
CREATE SCHEMA ShopSchema;

SET SEARCH_PATH = 'shopschema';

CREATE EXTENSION IF NOT EXISTS dblink;

CREATE TYPE MainDB.shopschema.SEX AS ENUM
(
  'Man',
  'Woman'
);

CREATE OR REPLACE FUNCTION cast_sex_to_varchar(sex) RETURNS VARCHAR AS $$
begin
  IF $1 = 'Man' THEN
    RETURN 'Man';
  ELSE
    RETURN 'Woman';
  end if;
end;
$$ LANGUAGE plpgsql;

CREATE CAST (MainDB.shopschema.SEX AS VARCHAR)
WITH FUNCTION cast_sex_to_varchar(sex);

CREATE TYPE Maindb.shopschema.COLOR AS ENUM
(
  'Black',
  'DarkBlue',
  'DarkGreen',
  'DarkCyan',
  'DarkRed',
  'DarkMagenta',
  'DarkYellow',
  'Gray',
  'DarkGray',
  'Blue',
  'Green',
  'Cyan',
  'Red',
  'Magenta',
  'Yellow',
  'White'
);

CREATE TYPE Maindb.shopschema.ITEM_TYPE AS ENUM
(
  'Jeans',
  'T-shirt',
  'Shirt',
  'Boots',
  'Accessory',
  'Pants'
);

CREATE TYPE Maindb.shopschema.MODEL AS ENUM
(
  '501',
  '502',
  '505',
  '511',
  '512'
);


CREATE SEQUENCE IF NOT EXISTS MainDB.shopschema.shop_codes
  AS INT
  MINVALUE 0
  NO MAXVALUE
  NO CYCLE;

CREATE TABLE IF NOT EXISTS MainDB.shopschema.Shops
(
  shopCode INT PRIMARY KEY NOT NULL DEFAULT nextval('MainDB.shopschema.shop_codes'),
  shopName VARCHAR(25) UNIQUE NOT NULL,
  isOutlet BOOLEAN DEFAULT FALSE NOT NULL,
  city VARCHAR(50) NOT NULL,
  address  VARCHAR(25) NOT NULL,
  isClosed BOOLEAN NOT NULL DEFAULT FALSE,
  openDate DATE NOT NULL,

  stats JSONB NOT NULL DEFAULT '{}',

  directorFirstName VARCHAR(25) NOT NULL,
  directorLastName VARCHAR(25) NOT NULL,
  directorMiddleName VARCHAR(25) NOT NULL,
  directorPhone CHAR(11) NOT NULL,
  directorDateOfBirth DATE NOT NULL,

  area REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS MainDB.shopschema.Shops_Stats_Years
(
  year INT DEFAULT EXTRACT(YEAR FROM current_date) NOT NULL,
  shopCode INT NOT NULL,
  stats JSONB NOT NULL DEFAULT '{}',

  FOREIGN KEY (shopCode) REFERENCES MainDB.shopschema.Shops (shopcode),
  PRIMARY KEY (shopCode, year)
);

CREATE TABLE IF NOT EXISTS MainDB.shopschema.Shops_Stats_Months
(
  year INT DEFAULT EXTRACT(YEAR FROM current_date) NOT NULL,
  month INT DEFAULT EXTRACT(MONTH FROM current_date) NOT NULL,
  shopCode INT NOT NULL,
  stats JSONB NOT NULL DEFAULT '{}',

  FOREIGN KEY (shopCode) REFERENCES MainDB.shopschema.Shops (shopCode),
  PRIMARY KEY (shopCode, year, month)
);

CREATE TABLE IF NOT EXISTS MainDB.shopschema.Shops_Stats_Weeks
(
  year INT DEFAULT EXTRACT(YEAR FROM current_date) NOT NULL,
  week INT DEFAULT EXTRACT(WEEK FROM current_date) NOT NULL,
  shopCode INT NOT NULL,
  stats JSONB NOT NULL DEFAULT '{}',

  FOREIGN KEY (shopCode) REFERENCES MainDB.shopschema.Shops (shopCode),
  PRIMARY KEY (shopCode, year, week)
);

CREATE TABLE IF NOT EXISTS MainDB.shopschema.Card
(
  cardID INT PRIMARY KEY NOT NULL,
  phone CHAR(11) UNIQUE,
  firstName VARCHAR(25),
  lastName VARCHAR(25),
  discount SMALLINT NOT NULL DEFAULT 5 CHECK (discount > 0),
  sex ShopSchema.SEX,
  email VARCHAR(50),
  isWorker BOOLEAN NOT NULL,
  dateOfBirth DATE,
  city VARCHAR(50),
  totalSum MONEY,

  stats JSONB DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS MainDB.shopschema.Card_Stats_Years
(
  year INT DEFAULT EXTRACT(YEAR FROM current_date) NOT NULL,
  cardID INT NOT NULL,
  stats JSONB NOT NULL DEFAULT '{}',

  FOREIGN KEY (cardID) REFERENCES MainDB.shopschema.Card (cardid),
  PRIMARY KEY (cardID, year)
);

CREATE TABLE IF NOT EXISTS MainDB.shopschema.Card_Stats_Months
(
  year INT DEFAULT EXTRACT(YEAR FROM current_date) NOT NULL,
  month INT DEFAULT EXTRACT(MONTH FROM current_date) NOT NULL,
  cardID INT NOT NULL,
  stats JSONB NOT NULL DEFAULT '{}',

  FOREIGN KEY (cardID) REFERENCES MainDB.shopschema.Card (cardID),
  PRIMARY KEY (cardID, year, month)
);

CREATE TABLE IF NOT EXISTS MainDB.shopschema.Card_Stats_Weeks
(
  year INT DEFAULT EXTRACT(YEAR FROM current_date) NOT NULL,
  week INT DEFAULT EXTRACT(WEEK FROM current_date) NOT NULL,
  cardID INT NOT NULL,
  stats JSONB NOT NULL DEFAULT '{}',

  FOREIGN KEY (cardID) REFERENCES MainDB.shopschema.Card (cardID),
  PRIMARY KEY (cardID, year, week)
);

CREATE TABLE IF NOT EXISTS Maindb.shopschema.ItemType
(
  sku INT PRIMARY KEY NOT NULL,
  itemName VARCHAR(50) NOT NULL,
  description VARCHAR(100),
  sex SEX NOT NULL,
  type item_type NOT NULL,
  model MODEL NOT NULL
);

CREATE TABLE IF NOT EXISTS Maindb.shopschema.Item
(
  itemID INT NOT NULL,
  sku INT NOT NULL ,
  size SMALLINT NOT NULL ,
  color COLOR NOT NULL,

  PRIMARY KEY (itemID, sku),

  FOREIGN KEY (sku) REFERENCES MainDB.shopschema.ItemType (sku) ON DELETE CASCADE
);

SELECT stats->'statsOfYear' FROM MainDB.shopschema.Shops;

-- CREATE OR REPLACE FUNCTION MainDB.shopschema.dump_year(year_p INT, shopCode_p INT)
-- RETURNS VOID AS $$
-- BEGIN
--   INSERT INTO MainDB.shopschema.Shops_Stats_Years (year, shopcode, stats) VALUES (year_p, shopCode_p, (SELECT stats->'statsOfYear' FROM MainDB.shopschema.Shops WHERE shopCode = shopCode_p));
-- END;
-- $$ LANGUAGE plpgsql;
--
-- SELECT dump_year(2018, 100);

INSERT INTO MainDB.shopschema.Shops (shopcode, shopname, isoutlet, city, address, stats, directorfirstname, directorlastname, directormiddlename, directorphone, directordateofbirth, area)
    VALUES (100, 'Moscow 1', FALSE , 'Moscow', 'street', '{}', 'a','a','a','a', '02-12-1997', 100);

INSERT INTO MainDB.shopschema.Card (cardid, isWorker) VALUES
  (0, FALSE ), (1, FALSE), (2, FALSE), (3, FALSE), (4, FALSE);

SELECT * FROM MainDB.shopschema.Card WHERE cardid IN (1, 2);

UPDATE card SET totalsum = 0.0 WHERE cardid = 1;
UPDATE shops SET stats = jsonb_set(stats, '{statsOfDay, CR}'::text[], '1.0');
--UPDATE MainDB.shopschema.Shops SET stats =

SELECT stats->'statsOfWeek'->'CR' AS S FROM MainDB.shopschema.shops WHERE shopcode = 100;
UPDATE card SET stats = jsonb_set(stats, '{bought, 100, 111}', '2');
--UPDATE shops SET stats = jsonb_set(stats, '{statsOfWeek}', '1.0', FALSE);

SELECT * FROM card WHERE cardid::VARCHAR LIKE '100%';

SELECT stats FROM MainDB.shopschema.card_stats_months WHERE year = 2018 AND month IN (ARRAY [4, 5]);
SELECT (2018, 2017);


SELECT (stats->>'checkCount')::INT FROM MainDB.shopschema.Card_Stats_Years;
SELECT SUM((stats->>'totalSum')::FLOAT) s1, SUM((stats->>'checkCount')::INT) s2 FROM MainDB.shopschema.Card_Stats_Years WHERE year IN (2018) GROUP BY year;
SELECT SUM((stats->>'totalSum')::FLOAT), SUM((stats->>'checkCount')::INT) FROM MainDB.shopschema.Card_Stats_Months WHERE year = 2018 AND month IN (4, 5) GROUP BY month;

UPDATE MainDB.shopschema.ItemType SET itemName = itemName || ' ' || sex;


CREATE OR REPLACE FUNCTION MainDB.shopschema.get_sku_pairs_frequency_year(shopcode_p INT, years INT[])
RETURNS TABLE(item1 VARCHAR, item2 VARCHAR, sex VARCHAR, count BIGINT) AS $$
BEGIN
RETURN QUERY
SELECT (MainDB.shopschema.get_name_by_sku(substring(T.key, 2, 3)))[1],
  (MainDB.shopschema.get_name_by_sku(substring(T.key, 6, 3)))[1],
  (MainDB.shopschema.get_name_by_sku(substring(T.key, 6, 3)))[2], SUM(T.value::TEXT::INT) S FROM
  (SELECT (jsonb_each((stats->>'skuPairsFreq')::JSONB)).key, (jsonb_each((stats->>'skuPairsFreq')::JSONB)).value
   FROM MainDB.shopschema.Shops_Stats_Years WHERE years @> (ARRAY[]::INT[] || year) AND shopCode = shopcode_p) AS T GROUP BY T.key
  ORDER BY S DESC ;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION MainDB.shopschema.get_common_sku_pairs_frequency_year(years INT[], city INT DEFAULT 0)
RETURNS TABLE(item1 VARCHAR, item2 VARCHAR, sex VARCHAR, count BIGINT) AS $$
BEGIN
  IF city = 0 THEN
    RETURN QUERY
    SELECT (MainDB.shopschema.get_name_by_sku(substring(T.key, 2, 3)))[1],
    (MainDB.shopschema.get_name_by_sku(substring(T.key, 6, 3)))[1],
    (MainDB.shopschema.get_name_by_sku(substring(T.key, 6, 3)))[2], SUM(T.value::TEXT::INT) S FROM
    (SELECT (jsonb_each((stats->>'skuPairsFreq')::JSONB)).key, (jsonb_each((stats->>'skuPairsFreq')::JSONB)).value
     FROM MainDB.shopschema.Shops_Stats_Years WHERE
       years @> (ARRAY[]::INT[] || year)) AS T GROUP BY T.key
    ORDER BY S DESC ;
  ELSE
    RETURN QUERY
    SELECT (MainDB.shopschema.get_name_by_sku(substring(T.key, 2, 3)))[1],
    (MainDB.shopschema.get_name_by_sku(substring(T.key, 6, 3)))[1],
    (MainDB.shopschema.get_name_by_sku(substring(T.key, 6, 3)))[2], SUM(T.value::TEXT::INT) S FROM
    (SELECT (jsonb_each((stats->>'skuPairsFreq')::JSONB)).key, (jsonb_each((stats->>'skuPairsFreq')::JSONB)).value
     FROM MainDB.shopschema.Shops_Stats_Years WHERE
       years @> (ARRAY[]::INT[] || year)
       AND shopCode / 100 = city) AS T GROUP BY T.key
    ORDER BY S DESC ;
  END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION MainDB.shopschema.get_sku_pairs_frequency_month(shopcode_p INT, year_p INT, months INT[])
RETURNS TABLE(item1 VARCHAR, item2 VARCHAR, sex VARCHAR, count BIGINT) AS $$
BEGIN
RETURN QUERY
SELECT (MainDB.shopschema.get_name_by_sku(substring(T.key, 2, 3)))[1],
  (MainDB.shopschema.get_name_by_sku(substring(T.key, 6, 3)))[1],
  (MainDB.shopschema.get_name_by_sku(substring(T.key, 6, 3)))[2], SUM(T.value::TEXT::INT) S FROM
  (SELECT (jsonb_each((stats->>'skuPairsFreq')::JSONB)).key, (jsonb_each((stats->>'skuPairsFreq')::JSONB)).value
   FROM MainDB.shopschema.Shops_Stats_Months WHERE
     year = year_p
     AND months @> (ARRAY[]::INT[] || month)
     AND shopCode = shopcode_p) AS T GROUP BY T.key
  ORDER BY S DESC ;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION MainDB.shopschema.get_common_sku_pairs_frequency_month(year_p INT, months INT[], city INT DEFAULT 0)
RETURNS TABLE(item1 VARCHAR, item2 VARCHAR, sex VARCHAR, count BIGINT) AS $$
BEGIN
IF city = 0 THEN
  RETURN QUERY
    SELECT (MainDB.shopschema.get_name_by_sku(substring(T.key, 2, 3)))[1],
    (MainDB.shopschema.get_name_by_sku(substring(T.key, 6, 3)))[1],
    (MainDB.shopschema.get_name_by_sku(substring(T.key, 6, 3)))[2], SUM(T.value::TEXT::INT) S FROM
    (SELECT (jsonb_each((stats->>'skuPairsFreq')::JSONB)).key, (jsonb_each((stats->>'skuPairsFreq')::JSONB)).value
     FROM MainDB.shopschema.Shops_Stats_Months WHERE
       year = year_p
       AND months @> (ARRAY[]::INT[] || month)) AS T GROUP BY T.key
    ORDER BY S DESC ;
ELSE RETURN QUERY
  SELECT (MainDB.shopschema.get_name_by_sku(substring(T.key, 2, 3)))[1],
    (MainDB.shopschema.get_name_by_sku(substring(T.key, 6, 3)))[1],
    (MainDB.shopschema.get_name_by_sku(substring(T.key, 6, 3)))[2], SUM(T.value::TEXT::INT) S FROM
    (SELECT (jsonb_each((stats->>'skuPairsFreq')::JSONB)).key, (jsonb_each((stats->>'skuPairsFreq')::JSONB)).value
     FROM MainDB.shopschema.Shops_Stats_Months WHERE
       year = year_p
       AND months @> (ARRAY[]::INT[] || month)
       AND shopCode / 100 = city) AS T GROUP BY T.key
    ORDER BY S DESC ;
END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION MainDB.shopschema.get_sku_pairs_frequency_week(shopcode_p INT, year_p INT, weeks INT[])
RETURNS TABLE(item1 VARCHAR, item2 VARCHAR, sex VARCHAR, count BIGINT) AS $$
BEGIN
RETURN QUERY
SELECT (MainDB.shopschema.get_name_by_sku(substring(T.key, 2, 3)))[1],
  (MainDB.shopschema.get_name_by_sku(substring(T.key, 6, 3)))[1],
  (MainDB.shopschema.get_name_by_sku(substring(T.key, 6, 3)))[2], SUM(T.value::TEXT::INT) S FROM
  (SELECT (jsonb_each((stats->>'skuPairsFreq')::JSONB)).key, (jsonb_each((stats->>'skuPairsFreq')::JSONB)).value
   FROM MainDB.shopschema.Shops_Stats_Weeks WHERE
     year = year_p
     AND weeks @> (ARRAY[]::INT[] || week)
     AND shopCode = shopcode_p) AS T GROUP BY T.key
  ORDER BY S DESC ;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION MainDB.shopschema.get_common_sku_pairs_frequency_week(year_p INT, weeks INT[], city INT DEFAULT 0)
RETURNS TABLE(item1 VARCHAR, item2 VARCHAR, sex VARCHAR, count BIGINT) AS $$
BEGIN
IF city = 0 THEN
  RETURN QUERY
  SELECT (MainDB.shopschema.get_name_by_sku(substring(T.key, 2, 3)))[1],
  (MainDB.shopschema.get_name_by_sku(substring(T.key, 6, 3)))[1],
  (MainDB.shopschema.get_name_by_sku(substring(T.key, 6, 3)))[2], SUM(T.value::TEXT::INT) S FROM
  (SELECT (jsonb_each((stats->>'skuPairsFreq')::JSONB)).key, (jsonb_each((stats->>'skuPairsFreq')::JSONB)).value
   FROM MainDB.shopschema.Shops_Stats_Weeks WHERE
     year = year_p
     AND weeks @> (ARRAY[]::INT[] || week)) AS T GROUP BY T.key
  ORDER BY S DESC ;
ELSE RETURN QUERY
SELECT (MainDB.shopschema.get_name_by_sku(substring(T.key, 2, 3)))[1],
  (MainDB.shopschema.get_name_by_sku(substring(T.key, 6, 3)))[1],
  (MainDB.shopschema.get_name_by_sku(substring(T.key, 6, 3)))[2], SUM(T.value::TEXT::INT) S FROM
  (SELECT (jsonb_each((stats->>'skuPairsFreq')::JSONB)).key, (jsonb_each((stats->>'skuPairsFreq')::JSONB)).value
   FROM MainDB.shopschema.Shops_Stats_Weeks WHERE
     year = year_p
     AND weeks @> (ARRAY[]::INT[] || week)
     AND shopCode / 100 = city) AS T GROUP BY T.key
  ORDER BY S DESC ;
END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION MainDB.shopschema.get_sku_frequency_year(shopcode_p INT, years INT[])
RETURNS TABLE (sex VARCHAR, item VARCHAR, count BIGINT) AS $$
BEGIN
  RETURN QUERY
  SELECT (MainDB.shopschema.get_name_by_sku(T.key))[2], (MainDB.shopschema.get_name_by_sku(T.key))[1], SUM(T.value::TEXT::INT) S FROM
  (SELECT (jsonb_each((stats->>'skuFreq')::JSONB)).key, (jsonb_each((stats->>'skuFreq')::JSONB)).value
  FROM MainDB.shopschema.Shops_Stats_Years WHERE years @> (ARRAY[]::INT[] || year) AND shopCode = shopcode_p) AS T GROUP BY T.key
  ORDER BY S DESC;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION MainDB.shopschema.get_common_sku_frequency_year(years INT[], city INT DEFAULT 0)
RETURNS TABLE (sex VARCHAR, item VARCHAR, count BIGINT) AS $$
BEGIN
  IF city = 0 THEN
    RETURN QUERY
    SELECT (MainDB.shopschema.get_name_by_sku(T.key))[2], (MainDB.shopschema.get_name_by_sku(T.key))[1], SUM(T.value::TEXT::INT) S FROM
    (SELECT (jsonb_each((stats->>'skuFreq')::JSONB)).key, (jsonb_each((stats->>'skuFreq')::JSONB)).value
    FROM MainDB.shopschema.Shops_Stats_Years WHERE
      years @> (ARRAY[]::INT[] || year)) AS T GROUP BY T.key
    ORDER BY S DESC;
  ELSE RETURN QUERY
    SELECT (MainDB.shopschema.get_name_by_sku(T.key))[2], (MainDB.shopschema.get_name_by_sku(T.key))[1], SUM(T.value::TEXT::INT) S FROM
    (SELECT (jsonb_each((stats->>'skuFreq')::JSONB)).key, (jsonb_each((stats->>'skuFreq')::JSONB)).value
    FROM MainDB.shopschema.Shops_Stats_Years WHERE
      years @> (ARRAY[]::INT[] || year)
      AND shopCode / 100 = city) AS T GROUP BY T.key
    ORDER BY S DESC;
  END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION MainDB.shopschema.get_sku_frequency_month(shopcode_p INT, year_p INT, months INT[])
RETURNS TABLE (sex VARCHAR, item VARCHAR, count BIGINT) AS $$
BEGIN
  RETURN QUERY
  SELECT (MainDB.shopschema.get_name_by_sku(T.key))[2], (MainDB.shopschema.get_name_by_sku(T.key))[1], SUM(T.value::TEXT::INT) S FROM
  (SELECT (jsonb_each((stats->>'skuFreq')::JSONB)).key, (jsonb_each((stats->>'skuFreq')::JSONB)).value
  FROM MainDB.shopschema.Shops_Stats_Months WHERE
    year = year_p
    AND months @> (ARRAY[]::INT[] || month)
    AND shopCode = shopcode_p) AS T GROUP BY T.key
  ORDER BY S DESC;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION MainDB.shopschema.get_common_sku_frequency_month(year_p INT, months INT[], city INT DEFAULT 0)
RETURNS TABLE (sex VARCHAR, item VARCHAR, count BIGINT) AS $$
BEGIN
  IF city = 0 THEN
    RETURN QUERY
    SELECT (MainDB.shopschema.get_name_by_sku(T.key))[2], (MainDB.shopschema.get_name_by_sku(T.key))[1], SUM(T.value::TEXT::INT) S FROM
    (SELECT (jsonb_each((stats->>'skuFreq')::JSONB)).key, (jsonb_each((stats->>'skuFreq')::JSONB)).value
    FROM MainDB.shopschema.Shops_Stats_Months WHERE
      year = year_p
      AND months @> (ARRAY[]::INT[] || month)) AS T GROUP BY T.key
    ORDER BY S DESC;
  ELSE RETURN QUERY
    SELECT (MainDB.shopschema.get_name_by_sku(T.key))[2], (MainDB.shopschema.get_name_by_sku(T.key))[1], SUM(T.value::TEXT::INT) S FROM
    (SELECT (jsonb_each((stats->>'skuFreq')::JSONB)).key, (jsonb_each((stats->>'skuFreq')::JSONB)).value
    FROM MainDB.shopschema.Shops_Stats_Months WHERE
      year = year_p
      AND months @> (ARRAY[]::INT[] || month)
      AND shopCode / 100 = city) AS T GROUP BY T.key
    ORDER BY S DESC;
  END IF;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION MainDB.shopschema.get_sku_frequency_week(shopcode_p INT, year_p INT, weeks INT[])
RETURNS TABLE (sex VARCHAR, item VARCHAR, count BIGINT) AS $$
BEGIN
  RETURN QUERY
  SELECT (MainDB.shopschema.get_name_by_sku(T.key))[2], (MainDB.shopschema.get_name_by_sku(T.key))[1], SUM(T.value::TEXT::INT) S FROM
  (SELECT (jsonb_each((stats->>'skuFreq')::JSONB)).key, (jsonb_each((stats->>'skuFreq')::JSONB)).value
  FROM MainDB.shopschema.Shops_Stats_Weeks WHERE
    year = year_p
    AND weeks @> (ARRAY[]::INT[] || week)
    AND shopCode = shopcode_p) AS T GROUP BY T.key
  ORDER BY S DESC;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION MainDB.shopschema.get_common_sku_frequency_week(year_p INT, weeks INT[], city INT DEFAULT 0)
RETURNS TABLE (sex VARCHAR, item VARCHAR, count BIGINT) AS $$
BEGIN
  IF city = 0 THEN
    RETURN QUERY
    SELECT (MainDB.shopschema.get_name_by_sku(T.key))[2], (MainDB.shopschema.get_name_by_sku(T.key))[1], SUM(T.value::TEXT::INT) S FROM
    (SELECT (jsonb_each((stats->>'skuFreq')::JSONB)).key, (jsonb_each((stats->>'skuFreq')::JSONB)).value
    FROM MainDB.shopschema.Shops_Stats_Weeks WHERE
      year = year_p
      AND weeks @> (ARRAY[]::INT[] || week)) AS T GROUP BY T.key
    ORDER BY S DESC;
  ELSE RETURN QUERY
    SELECT (MainDB.shopschema.get_name_by_sku(T.key))[2], (MainDB.shopschema.get_name_by_sku(T.key))[1], SUM(T.value::TEXT::INT) S FROM
    (SELECT (jsonb_each((stats->>'skuFreq')::JSONB)).key, (jsonb_each((stats->>'skuFreq')::JSONB)).value
    FROM MainDB.shopschema.Shops_Stats_Weeks WHERE
      year = year_p
      AND weeks @> (ARRAY[]::INT[] || week)
      AND shopCode / 100 = city) AS T GROUP BY T.key
    ORDER BY S DESC;
  END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION MainDB.shopschema.get_name_by_sku(sku_p VARCHAR) RETURNS VARCHAR[] AS $$
DECLARE
  name VARCHAR;
  sexp VARCHAR;
BEGIN
  SELECT itemName, sex INTO name, sexp FROM MainDB.shopschema.ItemType WHERE sku = sku_p::INT;
  RETURN ARRAY[]::VARCHAR[] || name || sexp;
END;
$$ LANGUAGE plpgsql;

SELECT MainDB.shopschema.get_sku_pairs_frequency_year(2018, '{1,2,4,5,6}'::INT[]);
SELECT jsonb_each((stats->>'skuPairsFreq')::JSONB) FROM MainDB.shopschema.Shops_Stats_Years WHERE '{2018}'::INT[] @> (ARRAY[]::INT[] || year)
                                                                                      AND shopCode = 100;

SELECT MainDB.shopschema.get_name_by_sku(substring(T.key, 2, 3)),
  MainDB.shopschema.get_name_by_sku(substring(T.key, 6, 3)), SUM(T.value::TEXT::INT) FROM
  (SELECT (jsonb_each((stats->>'skuPairsFreq')::JSONB)).key, (jsonb_each((stats->>'skuPairsFreq')::JSONB)).value
FROM MainDB.shopschema.Shops_Stats_Years WHERE '{2018, 2017}'::INT[] @> (ARRAY[]::INT[] || year) AND shopCode = 100) AS T GROUP BY T.key;
SELECT MainDB.shopschema.get_name_by_sku('111');

SELECT MainDB.shopschema.get_sku_pairs_frequency_year(100, '{2018, 2017}'::INT[]);

SELECT '{1,2,4,5,6}'::INT[] @> (ARRAY[]::INT[] || 1);

SELECT (MainDB.shopschema.get_name_by_sku(T.key))[2], (MainDB.shopschema.get_name_by_sku(T.key))[1], SUM(T.value::TEXT::INT) FROM
(SELECT (jsonb_each((stats->>'skuFreq')::JSONB)).key, (jsonb_each((stats->>'skuFreq')::JSONB)).value
FROM MainDB.shopschema.Shops_Stats_Years WHERE '{2018, 2017}'::INT[] @> (ARRAY[]::INT[] || year) AND shopCode = 100) AS T GROUP BY T.key;

SELECT MainDB.shopschema.get_sku_frequency_year(100, '{2018, 2017}'::INT[]);
SELECT MainDB.shopschema.get_sku_frequency_month(100, '{2018}'::INT[], '{4, 5}'::INT[]);

SELECT SUM((stats->>'CR')::FLOAT), SUM((stats->>'UPT')::FLOAT), SUM((stats->>'avgCheck')::FLOAT) FROM MainDB.shopschema.Shops_Stats_Years WHERE year IN (2018, 2017);

CREATE OR REPLACE FUNCTION MainDB.shopschema.get_city_shop_stats_year(years INT[], city_p INT DEFAULT 0)
RETURNS TABLE (CR FLOAT, UPT FLOAT, avgCheck FLOAT, salesPerArea FLOAT, countOfChecks BIGINT, returnedUnits BIGINT,
  countOfVisitors BIGINT, proceedsWithTax FLOAT, proceedsWithoutTax FLOAT, countOfSoldUnits BIGINT) AS $$
BEGIN
  IF city_p != 0 THEN
    RETURN QUERY
    SELECT (SUM((stats->>'countOfChecks')::FLOAT) / SUM((stats->>'countOfVisitors')::INT)),
      (SUM((stats->>'countOfSoldUnits')::FLOAT) / SUM((stats->>'countOfVisitors')::INT)),
      (SUM((stats->>'proceedsWithTax')::FLOAT) / SUM((stats->>'countOfChecks')::INT)),
      SUM((stats->>'salesPerArea')::FLOAT), SUM((stats->>'countOfChecks')::INT),
      SUM((stats->>'returnedUnits')::INT), SUM((stats->>'countOfVisitors')::INT),
      SUM((stats->>'proceedsWithoutTax')::FLOAT), SUM((stats->>'proceedsWithTax')::FLOAT),
      SUM((stats->>'countOfSoldUnits')::INT)
    FROM MainDB.shopschema.Shops_Stats_Years WHERE
      years @> (ARRAY[]::INT[] || year)
      AND shopcode / 100 = city_p
    GROUP BY year;
  ELSE
     RETURN QUERY
    SELECT (SUM((stats->>'countOfChecks')::FLOAT) / SUM((stats->>'countOfVisitors')::INT)),
      (SUM((stats->>'countOfSoldUnits')::FLOAT) / SUM((stats->>'countOfVisitors')::INT)),
      (SUM((stats->>'proceedsWithTax')::FLOAT) / SUM((stats->>'countOfChecks')::INT)),
      SUM((stats->>'salesPerArea')::FLOAT), SUM((stats->>'countOfChecks')::INT),
      SUM((stats->>'returnedUnits')::INT), SUM((stats->>'countOfVisitors')::INT),
      SUM((stats->>'proceedsWithoutTax')::FLOAT), SUM((stats->>'proceedsWithTax')::FLOAT),
      SUM((stats->>'countOfSoldUnits')::INT)
    FROM MainDB.shopschema.Shops_Stats_Years WHERE years @> (ARRAY[]::INT[] || year) GROUP BY year;
  END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION MainDB.shopschema.get_city_shop_stats_month(year_p INT, months INT[], city_p INT DEFAULT 0)
RETURNS TABLE (CR FLOAT, UPT FLOAT, avgCheck FLOAT, salesPerArea FLOAT, countOfChecks BIGINT, returnedUnits BIGINT,
  countOfVisitors BIGINT, proceedsWithTax FLOAT, proceedsWithoutTax FLOAT, countOfSoldUnits BIGINT) AS $$
BEGIN
  IF city_p != 0 THEN
    RETURN QUERY
    SELECT (SUM((stats->>'countOfChecks')::FLOAT) / SUM((stats->>'countOfVisitors')::INT)),
      (SUM((stats->>'countOfSoldUnits')::FLOAT) / SUM((stats->>'countOfVisitors')::INT)),
      (SUM((stats->>'proceedsWithTax')::FLOAT) / SUM((stats->>'countOfChecks')::INT)),
      SUM((stats->>'salesPerArea')::FLOAT), SUM((stats->>'countOfChecks')::INT),
      SUM((stats->>'returnedUnits')::INT), SUM((stats->>'countOfVisitors')::INT),
      SUM((stats->>'proceedsWithoutTax')::FLOAT), SUM((stats->>'proceedsWithTax')::FLOAT),
      SUM((stats->>'countOfSoldUnits')::INT)
    FROM MainDB.shopschema.Shops_Stats_Months WHERE
      year_p = year
      AND months @> (ARRAY[]::INT[] || month)
      AND shopcode / 100 = city_p
    GROUP BY month;
  ELSE
     RETURN QUERY
    SELECT (SUM((stats->>'countOfChecks')::FLOAT) / SUM((stats->>'countOfVisitors')::INT)),
      (SUM((stats->>'countOfSoldUnits')::FLOAT) / SUM((stats->>'countOfVisitors')::INT)),
      (SUM((stats->>'proceedsWithTax')::FLOAT) / SUM((stats->>'countOfChecks')::INT)),
      SUM((stats->>'salesPerArea')::FLOAT), SUM((stats->>'countOfChecks')::INT),
      SUM((stats->>'returnedUnits')::INT), SUM((stats->>'countOfVisitors')::INT),
      SUM((stats->>'proceedsWithoutTax')::FLOAT), SUM((stats->>'proceedsWithTax')::FLOAT),
      SUM((stats->>'countOfSoldUnits')::INT)
    FROM MainDB.shopschema.Shops_Stats_Months WHERE
      year_p = year
      AND months @> (ARRAY[]::INT[] || month)
    GROUP BY month;
  END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION MainDB.shopschema.get_city_shop_stats_week(year_p INT, weeks INT[], city_p INT DEFAULT 0)
RETURNS TABLE (CR FLOAT, UPT FLOAT, avgCheck FLOAT, salesPerArea FLOAT, countOfChecks BIGINT, returnedUnits BIGINT,
  countOfVisitors BIGINT, proceedsWithTax FLOAT, proceedsWithoutTax FLOAT, countOfSoldUnits BIGINT) AS $$
BEGIN
  IF city_p != 0 THEN
    RETURN QUERY
    SELECT (SUM((stats->>'countOfChecks')::FLOAT) / SUM((stats->>'countOfVisitors')::INT)),
      (SUM((stats->>'countOfSoldUnits')::FLOAT) / SUM((stats->>'countOfVisitors')::INT)),
      (SUM((stats->>'proceedsWithTax')::FLOAT) / SUM((stats->>'countOfChecks')::INT)),
      SUM((stats->>'salesPerArea')::FLOAT), SUM((stats->>'countOfChecks')::INT),
      SUM((stats->>'returnedUnits')::INT), SUM((stats->>'countOfVisitors')::INT),
      SUM((stats->>'proceedsWithoutTax')::FLOAT), SUM((stats->>'proceedsWithTax')::FLOAT),
      SUM((stats->>'countOfSoldUnits')::INT)
    FROM MainDB.shopschema.Shops_Stats_Weeks WHERE
      year_p = year
      AND weeks @> (ARRAY[]::INT[] || week)
      AND shopcode / 100 = city_p
    GROUP BY week;
  ELSE
     RETURN QUERY
    SELECT (SUM((stats->>'countOfChecks')::FLOAT) / SUM((stats->>'countOfVisitors')::INT)),
      (SUM((stats->>'countOfSoldUnits')::FLOAT) / SUM((stats->>'countOfVisitors')::INT)),
      (SUM((stats->>'proceedsWithTax')::FLOAT) / SUM((stats->>'countOfChecks')::INT)),
      SUM((stats->>'salesPerArea')::FLOAT), SUM((stats->>'countOfChecks')::INT),
      SUM((stats->>'returnedUnits')::INT), SUM((stats->>'countOfVisitors')::INT),
      SUM((stats->>'proceedsWithoutTax')::FLOAT), SUM((stats->>'proceedsWithTax')::FLOAT),
      SUM((stats->>'countOfSoldUnits')::INT)
    FROM MainDB.shopschema.Shops_Stats_Weeks WHERE
      year_p = year
      AND weeks @> (ARRAY[]::INT[] || week)
    GROUP BY week;
  END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION MainDB.shopschema.get_card_times(shopName VARCHAR)
RETURNS RECORD AS $$
  DECLARE shopCode INTEGER;
          rec      RECORD;
  BEGIN
    SELECT INTO shopCode ShopSchema.shops.shopcode FROM ShopSchema.shops WHERE ShopSchema.shops.shopname = $1;
    SELECT INTO rec ARRAY (SELECT DISTINCT year FROM ShopSchema.card_stats_years WHERE cardid % 1000 = shopCode ORDER BY year ASC),
            ARRAY (SELECT DISTINCT month FROM ShopSchema.card_stats_months WHERE cardid % 1000 = shopCode ORDER BY month ASC),
            ARRAY (SELECT DISTINCT week FROM ShopSchema.card_stats_weeks WHERE cardid % 1000 = shopCode ORDER BY week ASC);
    RETURN rec;
  END;
$$ LANGUAGE plpgsql;


SELECT MainDB.shopschema.get_card_times('Moscow 1');
SELECT  round((stats->>'countOfChecks')::FLOAT) FROM MainDB.shopschema.Shops_Stats_Years;
UPDATE MainDB.shopschema.Shops_Stats_Weeks SET stats = jsonb_set(stats, '{avgCheck}', to_jsonb((stats->>'avgCheck')::FLOAT / 10));
SELECT (ARRAY (SELECT year FROM ShopSchema.card_stats_years WHERE cardid % 1000 = 101),  ARRAY (SELECT DISTINCT month FROM ShopSchema.card_stats_months WHERE cardid % 1000 = 101));
SELECT MainDB.shopschema.get_common_sku_pairs_frequency_year('{2018}'::INT[], 1);
SELECT MainDB.shopschema.get_city_shop_stats_year('{2018}'::INT[], 1);
SELECT MainDB.shopschema.get_city_shop_stats_month(2018, '{4, 5}'::INT[], 1);

SELECT MainDB.shopschema.get_city_shop_stats_year('{2018}'::INT[]);

SELECT year FROM MainDB.shopschema.Shops_Stats_Years JOIN MainDB.shopschema.Shops
    ON shops_stats_years.shopcode = shops.shopcode WHERE 'Moscow 1' = shopname

INSERT INTO MainDB.shopschema.Card_Stats_Years (year, cardid, stats) VALUES
DELETE FROM MainDB.shopschema.Card_Stats_Weeks WHERE year = 2019;

SELECT (stats->>'proceedsWithTax')::FLOAT, (stats->>'countOfVisitors')::INT
FROM MainDB.shopschema.Shops_Stats_Months WHERE year = 2018 AND month IN (1, 2, 3) AND shopcode = 100;
SELECT SUM((stats->>'totalSum')::FLOAT), SUM((stats->>'checkCount')::INT)
FROM MainDB.shopschema.Card_Stats_Years WHERE (year IN (2016, 2017, 2018)) AND cardid % 1000 = 100 GROUP BY year;

UPDATE MainDB.shopschema.Shops_Stats_Years SET stats = jsonb_set(stats, '{proceedsWithTax}', to_jsonb((stats->>'proceedsWithTax')::REAL / 4.3));
UPDATE MainDB.shopschema.Shops_Stats_Months SET stats = jsonb_set(stats, '{countOfVisitors}', to_jsonb((stats->>'countOfVisitors')::INT * 10));
UPDATE MainDB.shopschema.Card_Stats_Weeks  SET stats = jsonb_set(stats, '{checkCount}', to_jsonb((stats->>'checkCount')::REAL * 10))



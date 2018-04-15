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

  costs JSONB NOT NULL,

  area FLOAT NOT NULL
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

CREATE OR REPLACE FUNCTION MainDB.shopschema.get_sku_pairs_frequency_month(shopcode_p INT, years INT[], months INT[])
  RETURNS TABLE(item1 VARCHAR, item2 VARCHAR, sex VARCHAR, count BIGINT) AS $$
BEGIN
  RETURN QUERY
  SELECT (MainDB.shopschema.get_name_by_sku(substring(T.key, 2, 3)))[1],
    (MainDB.shopschema.get_name_by_sku(substring(T.key, 6, 3)))[1],
    (MainDB.shopschema.get_name_by_sku(substring(T.key, 6, 3)))[2], SUM(T.value::TEXT::INT) S FROM
    (SELECT (jsonb_each((stats->>'skuPairsFreq')::JSONB)).key, (jsonb_each((stats->>'skuPairsFreq')::JSONB)).value
     FROM MainDB.shopschema.Shops_Stats_Months WHERE
       years @> (ARRAY[]::INT[] || year)
       AND months @> (ARRAY[]::INT[] || month)
       AND shopCode = shopcode_p) AS T GROUP BY T.key
  ORDER BY S DESC ;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION MainDB.shopschema.get_sku_pairs_frequency_week(shopcode_p INT, years INT[], weeks INT[])
  RETURNS TABLE(item1 VARCHAR, item2 VARCHAR, sex VARCHAR, count BIGINT) AS $$
BEGIN
  RETURN QUERY
  SELECT (MainDB.shopschema.get_name_by_sku(substring(T.key, 2, 3)))[1],
    (MainDB.shopschema.get_name_by_sku(substring(T.key, 6, 3)))[1],
    (MainDB.shopschema.get_name_by_sku(substring(T.key, 6, 3)))[2], SUM(T.value::TEXT::INT) S FROM
    (SELECT (jsonb_each((stats->>'skuPairsFreq')::JSONB)).key, (jsonb_each((stats->>'skuPairsFreq')::JSONB)).value
     FROM MainDB.shopschema.Shops_Stats_Weeks WHERE
       years @> (ARRAY[]::INT[] || year)
       AND weeks @> (ARRAY[]::INT[] || week)
       AND shopCode = shopcode_p) AS T GROUP BY T.key
  ORDER BY S DESC ;
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

CREATE OR REPLACE FUNCTION MainDB.shopschema.get_sku_frequency_month(shopcode_p INT, years INT[], months INT[])
  RETURNS TABLE (sex VARCHAR, item VARCHAR, count BIGINT) AS $$
BEGIN
  RETURN QUERY
  SELECT (MainDB.shopschema.get_name_by_sku(T.key))[2], (MainDB.shopschema.get_name_by_sku(T.key))[1], SUM(T.value::TEXT::INT) S FROM
    (SELECT (jsonb_each((stats->>'skuFreq')::JSONB)).key, (jsonb_each((stats->>'skuFreq')::JSONB)).value
     FROM MainDB.shopschema.Shops_Stats_Months WHERE
       years @> (ARRAY[]::INT[] || year)
       AND months @> (ARRAY[]::INT[] || month)
       AND shopCode = shopcode_p) AS T GROUP BY T.key
  ORDER BY S DESC;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION MainDB.shopschema.get_sku_frequency_week(shopcode_p INT, years INT[], weeks INT[])
  RETURNS TABLE (sex VARCHAR, item VARCHAR, count BIGINT) AS $$
BEGIN
  RETURN QUERY
  SELECT (MainDB.shopschema.get_name_by_sku(T.key))[2], (MainDB.shopschema.get_name_by_sku(T.key))[1], SUM(T.value::TEXT::INT) S FROM
    (SELECT (jsonb_each((stats->>'skuFreq')::JSONB)).key, (jsonb_each((stats->>'skuFreq')::JSONB)).value
     FROM MainDB.shopschema.Shops_Stats_Weeks WHERE
       years @> (ARRAY[]::INT[] || year)
       AND weeks @> (ARRAY[]::INT[] || week)
       AND shopCode = shopcode_p) AS T GROUP BY T.key
  ORDER BY S DESC;
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
    FROM MainDB.shopschema.Shops_Stats_Years WHERE years @> (ARRAY[]::INT[] || year) AND shopcode / 100 = city_p;
  ELSE
    RETURN QUERY
    SELECT (SUM((stats->>'countOfChecks')::FLOAT) / SUM((stats->>'countOfVisitors')::INT)),
      (SUM((stats->>'countOfSoldUnits')::FLOAT) / SUM((stats->>'countOfVisitors')::INT)),
      (SUM((stats->>'proceedsWithTax')::FLOAT) / SUM((stats->>'countOfChecks')::INT)),
      SUM((stats->>'salesPerArea')::FLOAT), SUM((stats->>'countOfChecks')::INT),
      SUM((stats->>'returnedUnits')::INT), SUM((stats->>'countOfVisitors')::INT),
      SUM((stats->>'proceedsWithoutTax')::FLOAT), SUM((stats->>'proceedsWithTax')::FLOAT),
      SUM((stats->>'countOfSoldUnits')::INT)
    FROM MainDB.shopschema.Shops_Stats_Years WHERE years @> (ARRAY[]::INT[] || year);
  END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION MainDB.shopschema.get_city_shop_stats_month(years INT, months INT[], city_p INT DEFAULT 0)
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
      years = year
      AND months @> (ARRAY[]::INT[] || month)
      AND shopcode / 100 = city_p;
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
      years = year
      AND months @> (ARRAY[]::INT[] || month);
  END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION MainDB.shopschema.get_city_shop_stats_week(years INT, weeks INT[], city_p INT DEFAULT 0)
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
      years = year
      AND weeks @> (ARRAY[]::INT[] || week)
      AND shopcode / 100 = city_p;
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
      years = year
      AND weeks @> (ARRAY[]::INT[] || week);
  END IF;
END;
$$ LANGUAGE plpgsql;
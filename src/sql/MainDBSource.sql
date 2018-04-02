CREATE SCHEMA ShopSchema;

SET SEARCH_PATH = 'shopschema';

CREATE EXTENSION IF NOT EXISTS dblink;

CREATE TYPE MainDB.shopschema.SEX AS ENUM
(
  'Man',
  'Woman'
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

  FOREIGN KEY (year, shopCode) REFERENCES MainDB.shopschema.Shops_Stats_Years (year, shopCode),
  PRIMARY KEY (shopCode, year, month)
);

CREATE TABLE IF NOT EXISTS MainDB.shopschema.Shops_Stats_Weeks
(
  year INT DEFAULT EXTRACT(YEAR FROM current_date) NOT NULL,
  week INT DEFAULT EXTRACT(WEEK FROM current_date) NOT NULL,
  shopCode INT NOT NULL,
  stats JSONB NOT NULL DEFAULT '{}',

  FOREIGN KEY (year, shopCode) REFERENCES MainDB.shopschema.Shops_Stats_Years (year, shopCode),
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

  FOREIGN KEY (cardID, year) REFERENCES MainDB.shopschema.Card_Stats_Years (cardID, year),
  PRIMARY KEY (cardID, year, month)
);
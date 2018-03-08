CREATE SCHEMA ShopSchema;

SET SEARCH_PATH = 'shopschema';
CREATE EXTENSION IF NOT EXISTS dblink;

CREATE TYPE shopdb.shopschema.COLOR AS ENUM
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

CREATE TYPE shopdb.shopschema.SEX AS ENUM
(
  'Man',
  'Woman'
);

CREATE TYPE ShopDB.shopschema.ITEM_TYPE AS ENUM
(
  'Jeans',
  'T-shirt',
  'Shirt',
  'Boots',
  'Accessory',
  'Pants'
);

CREATE TYPE ShopDB.shopschema.MODEL AS ENUM
(
  '501',
  '502',
  '505',
  '511',
  '512'
);

CREATE SEQUENCE IF NOT EXISTS ShopSchema.emp_codes
  AS INT
  MINVALUE 0
  NO MAXVALUE
  NO CYCLE;

CREATE SEQUENCE IF NOT EXISTS ShopSchema.check_codes
  AS INT
  MINVALUE 0
  NO MAXVALUE
  NO CYCLE;

CREATE TABLE IF NOT EXISTS ShopDB.shopschema.Shop
(
  shopCode INT PRIMARY KEY NOT NULL,
  shopName VARCHAR(25) UNIQUE NOT NULL,
  isOutlet BOOLEAN DEFAULT FALSE NOT NULL,
  address  VARCHAR(25) NOT NULL,
  city VARCHAR(50) NOT NULL,
  isClosed BOOLEAN NOT NULL DEFAULT FALSE,

  area FLOAT NOT NULL,
  countOfVisitorsToday INT NOT NULL CHECK (countOfVisitorsToday >= 0)
);

CREATE TABLE IF NOT EXISTS ShopDB.shopschema.Employee
(
  employeeCode INT PRIMARY KEY NOT NULL DEFAULT nextval('ShopDB.shopschema.emp_codes'),
  firstName VARCHAR(25) NOT NULL,
  lastName VARCHAR(25) NOT NULL,
  middleName VARCHAR(25),
  dateOfBirth DATE NOT NULL,
  phone CHAR(11) NOT NULL UNIQUE,
  position VARCHAR(25) NOT NULL,
  isFired BOOLEAN DEFAULT FALSE NOT NULL,
  salary MONEY NOT NULL,
  sex SEX NOT NULL,
  chief INT,
  shopCode INT,

  FOREIGN KEY (shopCode) REFERENCES ShopDB.ShopSchema.Shop (shopCode) ON DELETE CASCADE,
  FOREIGN KEY (chief) REFERENCES ShopDB.shopschema.Employee (employeeCode)
);

CREATE TABLE IF NOT EXISTS ShopDB.shopschema.Check
(
  checkID INT PRIMARY KEY NOT NULL DEFAULT nextval('ShopDB.shopschema.check_codes'),
  date TIMESTAMP WITH TIME ZONE NOT NULL UNIQUE ,
  totalCostWithTax MONEY NOT NULL,
  totalCostWithoutTax MONEY NOT NULL,
  isByCard BOOLEAN NOT NULL DEFAULT FALSE,
  discount SMALLINT DEFAULT 0,
  employeeCode INT,
  shopCode INT,

  FOREIGN KEY (shopCode) REFERENCES ShopDB.shopschema.Shop (shopCode),
  FOREIGN KEY (employeeCode) REFERENCES ShopDB.ShopSchema.Employee (employeeCode)
);

CREATE OR REPLACE FUNCTION shopdb.shopschema.item_type_code_check(id INT, sex Sex, type item_type, model model) RETURNS BOOLEAN AS $$
DECLARE
  code VARCHAR := '';
BEGIN
  IF sex = 'Man' THEN
    code := code || '1';
  ELSEIF sex = 'Woman' THEN
    code := code || '2';
  ELSE
    RAISE EXCEPTION 'Invalid sex --> %', sex;
  END IF;

  IF type = 'Jeans' THEN
    code := code || '1';
  ELSEIF type = 'T-shirt' THEN
    code := code || '2';
  ELSEIF type = 'Shirt' THEN
    code := code || '3';
  ELSEIF type = 'Boots' THEN
    code := code || '4';
  ELSEIF type = 'Accessory' THEN
    code := code || '5';
  ELSEIF type = 'Pants' THEN
    code := code || '6';
  ELSE
    RAISE EXCEPTION 'Invalid type --> %', type;
  END IF;

  IF model = '501' THEN
    code := code || '1';
  ELSEIF model = '502' THEN
    code := code || '2';
  ELSEIF model = '505' THEN
    code := code || '3';
  ELSEIF model = '511' THEN
    code := code || '4';
  ELSEIF model = '512' THEN
    code := code || '5';
  ELSE
    RAISE EXCEPTION 'Invalid model --> %', model;
  END IF;

  IF code::INT = id THEN
    RETURN TRUE;
  ELSE
    RETURN FALSE;
  END IF;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE IF NOT EXISTS ShopDB.shopschema.ItemType
(
  sku INT PRIMARY KEY NOT NULL,
  itemName VARCHAR(50) NOT NULL,
  description VARCHAR(100),
  sex SEX NOT NULL,
  type item_type NOT NULL,
  model MODEL NOT NULL
);

CREATE TABLE IF NOT EXISTS ShopDB.shopschema.Item
(
  itemID INT NOT NULL,
  sku INT NOT NULL ,
  size SMALLINT NOT NULL ,
  color COLOR NOT NULL,

  PRIMARY KEY (itemID, sku),

  FOREIGN KEY (sku) REFERENCES shopdb.shopschema.ItemType (sku) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS ShopDB.shopschema.Store
(
  shopCode INT NOT NULL ,
  itemID INT NOT NULL ,
  sku INT NOT NULL ,
  count INT NOT NULL CHECK (count >= 0),
  price MONEY NOT NULL ,

  PRIMARY KEY (shopCode, itemID, sku),

  FOREIGN KEY (shopCode) REFERENCES shopdb.shopschema.Shop (shopcode) ON DELETE CASCADE,
  FOREIGN KEY (itemID, sku) REFERENCES shopdb.shopschema.Item (itemID, sku) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS ShopDB.shopschema.Check_Item_INT
(
  checkID INT NOT NULL,
  itemID INT NOT NULL,
  sku INT NOT NULL,
  count INT NOT NULL CHECK (count > 0),
  costOfPositionWithTax MONEY NOT NULL,
  costOfPositionWithoutTax MONEY NOT NULL,
  discountOfPosition SMALLINT NOT NULL CHECK (discountOfPosition >= 0) DEFAULT 0,
  isReturned BOOLEAN NOT NULL DEFAULT FALSE,

  PRIMARY KEY (checkID, itemID, sku),

  FOREIGN KEY (checkID) REFERENCES shopdb.shopschema.Check (checkid) ON DELETE CASCADE,
  FOREIGN KEY (itemID, sku) REFERENCES shopdb.shopschema.Item (itemID, sku)
);

CREATE TABLE IF NOT EXISTS ShopDB.shopschema.Card_Check_INT
(
  checkID INT NOT NULL ,
  cardID INT NOT NULL ,
  purchases INT[][2] NOT NULL ,

  PRIMARY KEY (checkID, cardID),

  FOREIGN KEY (checkID) REFERENCES shopdb.shopschema.Check (checkid) ON DELETE CASCADE
);

CREATE OR REPLACE FUNCTION shopdb.shopschema.update_shop() RETURNS TRIGGER AS $$
BEGIN
  IF NEW.shopcode != OLD.shopcode OR
     NEW.shopname != OLD.shopname OR
     NEW.isoutlet != OLD.isoutlet OR
     NEW.address != OLD.address OR
     NEW.city != OLD.city OR
     NEW.isclosed != OLD.isclosed OR
     NEW.area != OLD.area THEN
    RAISE EXCEPTION 'trying to update non-updateble field';
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER tr_update_shop BEFORE UPDATE
  ON ShopDB.shopschema.Shop
  FOR EACH ROW
EXECUTE PROCEDURE ShopDB.shopschema.update_shop();

CREATE OR REPLACE FUNCTION shopdb.shopschema.insert_shop() RETURNS TRIGGER AS $$
BEGIN
  IF (SELECT count(shopdb.shopschema.Shop.shopCode) FROM shopdb.shopschema.Shop) = 1 THEN
    RAISE EXCEPTION 'trying to insert more than one row in table shop';
  ELSEIF (new.shopcode, new.shopname, new.isoutlet, new.address, new.city, new.area) NOT IN
         (SELECT * FROM dblink('dbname=maindb port=5432 user=postgres password=0212', 'SELECT shopCode, shopName, isoutlet, address, city, area FROM ShopSchema.shops')
           AS shops(shopCode INT, shopname VARCHAR(25), isoutlet BOOLEAN, address VARCHAR(25), city VARCHAR(50), area FLOAT)) THEN
    RAISE EXCEPTION 'invalid shopcode';
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER tr_insert_shop BEFORE INSERT
  ON shopdb.shopschema.Shop
  FOR EACH ROW
EXECUTE PROCEDURE ShopDB.shopschema.insert_shop();

CREATE OR REPLACE FUNCTION shopdb.shopschema.delete_shop() RETURNS TRIGGER AS $$
BEGIN
  RAISE EXCEPTION 'trying to delete row in shop';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER tr_delete_shop BEFORE DELETE
  ON shopdb.shopschema.Shop
  FOR EACH ROW
EXECUTE PROCEDURE shopdb.shopschema.delete_shop();

CREATE OR REPLACE FUNCTION shopdb.shopschema.delete_empl() RETURNS TRIGGER AS $$
BEGIN
  UPDATE shopdb.shopschema.Employee SET isfired = TRUE WHERE employeeCode = old.employeecode;
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER tr_delete_emp BEFORE DELETE
  ON shopdb.shopschema.Employee
  FOR EACH ROW
EXECUTE PROCEDURE shopdb.shopschema.delete_empl();

CREATE OR REPLACE FUNCTION shopdb.shopschema.insert_item_type() RETURNS TRIGGER AS $$
BEGIN
  IF NOT (shopdb.shopschema.item_type_code_check(new.sku, new.sex, new.type, new.model)) THEN
    RAISE EXCEPTION 'invalid sku';
  ELSE
    RETURN NEW;
  END IF;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER tr_insert_item_type BEFORE INSERT
  ON shopdb.shopschema.ItemType
  FOR EACH ROW
EXECUTE PROCEDURE shopdb.shopschema.insert_item_type();

CREATE OR REPLACE FUNCTION item_id_check(itemid INT, size SMALLINT, color Color) RETURNS BOOLEAN AS $$
DECLARE
  code VARCHAR := '';
BEGIN
  IF color = 'Black' THEN
    code = code || '10';
  ELSEIF color = 'DarkBlue'THEN
    code = code || '11';
  ELSEIF color = 'DarkGreen' THEN
    code = code || '12';
  ELSEIF color = 'DarkCyan' THEN
    code = code || '13';
  ELSEIF color = 'DarkRed' THEN
    code = code || '14';
  ELSEIF color = 'DarkMagenta' THEN
    code = code || '15';
  ELSEIF color = 'DarkYellow' THEN
    code = code || '16';
  ELSEIF color = 'Gray' THEN
    code = code || '17';
  ELSEIF color = 'DarkGray' THEN
    code = code || '18';
  ELSEIF color = 'Blue' THEN
    code = code || '19';
  ELSEIF color = 'Green' THEN
    code = code || '20';
  ELSEIF color = 'Cyan' THEN
    code = code || '21';
  ELSEIF color = 'Red' THEN
    code = code || '22';
  ELSEIF color = 'Magenta' THEN
    code = code || '23';
  ELSEIF color = 'Yellow' THEN
    code = code || '24';
  ELSEIF color = 'White' THEN
    code = code || '25';
  ELSE
    RAISE EXCEPTION 'Invalid color --> %', color;
  END IF;

  IF (size != '-1') THEN
    code = code || size::VARCHAR;
  END IF;

  IF (code::INT = itemid) THEN
    RETURN TRUE;
  ELSE
    RETURN FALSE;
  END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION shopdb.shopschema.insert_item() RETURNS TRIGGER AS $$
BEGIN
  IF NOT (shopdb.shopschema.item_id_check(new.itemid, new.size, new.color)) THEN
    RAISE EXCEPTION 'invalid ID';
  ELSE
    RETURN NEW;
  END IF;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER tr_insert_item BEFORE INSERT
  ON shopdb.shopschema.Item
  FOR EACH ROW EXECUTE PROCEDURE shopdb.shopschema.insert_item();

CREATE OR REPLACE FUNCTION shopdb.shopschema.insert_card_int() RETURNS TRIGGER AS $$
BEGIN
  IF NOT new.cardid IN (SELECT cardid FROM dblink('dbname=maindb port=5432 user=postgres password=0212', 'SELECT cardid FROM ShopSchema.card')
    AS cards(cardid INT)) THEN
    RAISE EXCEPTION 'invalid cardID';
  ELSE
    RETURN NEW;
  END IF;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER tr_insert_card_int BEFORE INSERT
  ON shopdb.shopschema.Card_Check_INT
  FOR EACH ROW EXECUTE PROCEDURE shopdb.shopschema.insert_card_int();

--INDEXES AND VIEWS--
CREATE MATERIALIZED VIEW shopdb.shopschema.items
  AS
    SELECT c.checkid, c.itemid, c.sku, itemname, sex, type, model, size, color, count,
      costofpositionwithouttax, costofpositionwithtax , discountofposition, isreturned, date, isbycard,
      discount, totalcostwithouttax, totalcostwithtax
    FROM shopdb.shopschema.check_item_int c
      JOIN shopdb.shopschema.item i ON c.itemid = i.itemid AND c.sku = i.sku
      JOIN shopdb.shopschema.itemtype i2 ON i.sku = i2.sku
      JOIN shopdb.shopschema."check" ch ON c.checkid = ch.checkid ORDER BY checkid;

CREATE UNIQUE INDEX IF NOT EXISTS items_index ON shopdb.shopschema.items (checkid, itemid, sku);
CREATE INDEX IF NOT EXISTS items_date_index ON shopdb.shopschema.items (date);

CREATE MATERIALIZED VIEW shopdb.shopschema.cards_purchases AS
  SELECT cardid, c1.checkid, c2.date, unnest(purchases[:][1:1]) AS sku, unnest(purchases[:][2:2]) AS itemid, c2.totalcostwithtax
  FROM shopdb.shopschema.card_check_int c1 JOIN shopdb.shopschema."check" c2 ON c1.checkid = c2.checkid;

CREATE INDEX IF NOT EXISTS cards_date_index ON shopdb.shopschema.cards_purchases (date);
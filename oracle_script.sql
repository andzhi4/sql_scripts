PROMPT =========================================================================
PROMPT TABLE DEFINITIONS
PROMPT =========================================================================

PROMPT CREATE TABLE BATTLES
CREATE TABLE BATTLES (
  NAME VARCHAR2(20) NOT NULL,
  "date" TIMESTAMP(6) NOT NULL
)
  STORAGE (
    NEXT       1024 K
  )

/

PROMPT CREATE TABLE CLASSES
CREATE TABLE CLASSES (
  CLASS        VARCHAR2(50) NOT NULL,
  TYPE         VARCHAR2(2)  NOT NULL,
  COUNTRY      VARCHAR2(20) NOT NULL,
  NUMGUNS      INTEGER      NULL,
  BORE         FLOAT(63)    NULL,
  DISPLACEMENT INTEGER      NULL
)
  STORAGE (
    NEXT       1024 K
  )
/



PROMPT CREATE TABLE COMPANY
CREATE TABLE COMPANY (
  ID_COMP INTEGER  NOT NULL,
  NAME    CHAR(10) NOT NULL
)
  STORAGE (
    NEXT       1024 K
  )
/



PROMPT CREATE TABLE INCOME
CREATE TABLE INCOME (
  CODE  INTEGER      NOT NULL,
  POINT INTEGER      NOT NULL,
  "date"  TIMESTAMP(6) NOT NULL,
  INC   NUMBER(12,2) NOT NULL
)
  STORAGE (
    NEXT       1024 K
  )
/



PROMPT CREATE TABLE INCOME_O
CREATE TABLE INCOME_O (
  POINT INTEGER      NOT NULL,
  "date"  TIMESTAMP(6) NOT NULL,
  INC   NUMBER(12,2) NOT NULL
)
  STORAGE (
    NEXT       1024 K
  )
/


PROMPT CREATE TABLE LAPTOP
CREATE TABLE LAPTOP (
  CODE   INTEGER      NOT NULL,
  MODEL  VARCHAR2(50) NOT NULL,
  SPEED  INTEGER      NOT NULL,
  RAM    INTEGER      NOT NULL,
  HD     FLOAT(63)    NOT NULL,
  PRICE  NUMBER(12,2) NULL,
  SCREEN INTEGER      NOT NULL
)
  STORAGE (
    NEXT       1024 K
  )
/


PROMPT CREATE TABLE OUTCOME
CREATE TABLE OUTCOME (
  CODE  INTEGER      NOT NULL,
  POINT INTEGER      NOT NULL,
  "date"  TIMESTAMP(6) NOT NULL,
  "out"   NUMBER(12,2) NOT NULL
)
  STORAGE (
    NEXT       1024 K
  )
/


PROMPT CREATE TABLE OUTCOME_O
CREATE TABLE OUTCOME_O (
  POINT INTEGER      NOT NULL,
  "date"  TIMESTAMP(6) NOT NULL,
  "out"   NUMBER(12,2) NOT NULL
)
  STORAGE (
    NEXT       1024 K
  )
/


PROMPT CREATE TABLE OUTCOMES
CREATE TABLE OUTCOMES (
  SHIP   VARCHAR2(50) NOT NULL,
  BATTLE VARCHAR2(20) NOT NULL,
  RESULT VARCHAR2(10) NOT NULL
)
  STORAGE (
    NEXT       1024 K
  )
/


PROMPT CREATE TABLE PASS_IN_TRIP
CREATE TABLE PASS_IN_TRIP (
  TRIP_NO INTEGER      NOT NULL,
  "date"    TIMESTAMP(6) NOT NULL,
  ID_PSG  INTEGER      NOT NULL,
  PLACE   CHAR(10)     NOT NULL
)
  STORAGE (
    NEXT       1024 K
  )
/


PROMPT CREATE TABLE PASSENGER
CREATE TABLE PASSENGER (
  ID_PSG INTEGER  NOT NULL,
  NAME   CHAR(20) NOT NULL
)
  STORAGE (
    NEXT       1024 K
  )
/


PROMPT CREATE TABLE PC
CREATE TABLE PC (
  CODE  INTEGER      NOT NULL,
  MODEL VARCHAR2(50) NOT NULL,
  SPEED INTEGER      NOT NULL,
  RAM   INTEGER      NOT NULL,
  HD    FLOAT(63)    NOT NULL,
  CD    VARCHAR2(10) NOT NULL,
  PRICE NUMBER(12,2) NULL
)
  STORAGE (
    NEXT       1024 K
  )
/


PROMPT CREATE TABLE PRINTER
CREATE TABLE PRINTER (
  CODE  INTEGER      NOT NULL,
  MODEL VARCHAR2(50) NOT NULL,
  COLOR CHAR(1)      NOT NULL,
  TYPE  VARCHAR2(10) NOT NULL,
  PRICE NUMBER(12,2) NULL
)
  STORAGE (
    NEXT       1024 K
  )
/


PROMPT CREATE TABLE PRODUCT
CREATE TABLE PRODUCT (
  MAKER VARCHAR2(10) NOT NULL,
  MODEL VARCHAR2(50) NOT NULL,
  TYPE  VARCHAR2(50) NOT NULL
)
  STORAGE (
    NEXT       1024 K
  )
/


PROMPT CREATE TABLE SHIPS
CREATE TABLE SHIPS (
  NAME     VARCHAR2(50) NOT NULL,
  CLASS    VARCHAR2(50) NOT NULL,
  LAUNCHED INTEGER      NULL
)
  STORAGE (
    NEXT       1024 K
  )
/


PROMPT CREATE TABLE TRIP
CREATE TABLE TRIP (
  TRIP_NO   INTEGER      NOT NULL,
  ID_COMP   INTEGER      NOT NULL,
  PLANE     CHAR(10)     NOT NULL,
  TOWN_FROM CHAR(25)     NOT NULL,
  TOWN_TO   CHAR(25)     NOT NULL,
  TIME_OUT  TIMESTAMP(6) NOT NULL,
  TIME_IN   TIMESTAMP(6) NOT NULL
)
  STORAGE (
    NEXT       1024 K
  )
/


PROMPT CREATE TABLE UTB
CREATE TABLE UTB (
  B_DATETIME TIMESTAMP(6) NOT NULL,
  B_Q_ID     INTEGER      NOT NULL,
  B_V_ID     INTEGER      NOT NULL,
  B_VOL      INTEGER      NOT NULL
)
  STORAGE (
    NEXT       1024 K
  )
/


PROMPT CREATE TABLE UTQ
CREATE TABLE UTQ (
  Q_ID   INTEGER      NOT NULL,
  Q_NAME VARCHAR2(35) NOT NULL
)
  STORAGE (
    NEXT       1024 K
  )
/


PROMPT CREATE TABLE UTV
CREATE TABLE UTV (
  V_ID    INTEGER      NOT NULL,
  V_NAME  VARCHAR2(35) NOT NULL,
  V_COLOR CHAR(1)      NOT NULL
)
  STORAGE (
    NEXT       1024 K
  )
/

PROMPT =========================================================================
PROMPT PRIMARY AND UNIQUE CONSTRAINTS
PROMPT =========================================================================

PROMPT ALTER TABLE BATTLES ADD CONSTRAINT PK_BATTLES PRIMARY KEY
ALTER TABLE BATTLES
  ADD CONSTRAINT PK_BATTLES PRIMARY KEY (
    NAME
  )
  USING INDEX
    STORAGE (
      NEXT       1024 K
    )
/

PROMPT ALTER TABLE CLASSES ADD CONSTRAINT PK_CLASSES PRIMARY KEY
ALTER TABLE CLASSES
  ADD CONSTRAINT PK_CLASSES PRIMARY KEY (
    CLASS
  )
  USING INDEX
    STORAGE (
      NEXT       1024 K
    )
/

PROMPT ALTER TABLE COMPANY ADD CONSTRAINT PK2 PRIMARY KEY
ALTER TABLE COMPANY
  ADD CONSTRAINT PK2 PRIMARY KEY (
    ID_COMP
  )
  USING INDEX
    STORAGE (
      NEXT       1024 K
    )
/

PROMPT ALTER TABLE INCOME ADD CONSTRAINT PK_INCOME PRIMARY KEY
ALTER TABLE INCOME
  ADD CONSTRAINT PK_INCOME PRIMARY KEY (
    CODE
  )
  USING INDEX
    STORAGE (
      NEXT       1024 K
    )
/

PROMPT ALTER TABLE INCOME_O ADD CONSTRAINT PK_INCOME_O PRIMARY KEY
ALTER TABLE INCOME_O
  ADD CONSTRAINT PK_INCOME_O PRIMARY KEY (
    POINT,
    "date"
  )
  USING INDEX
    STORAGE (
      NEXT       1024 K
    )
/

PROMPT ALTER TABLE LAPTOP ADD CONSTRAINT PK_LAPTOP PRIMARY KEY
ALTER TABLE LAPTOP
  ADD CONSTRAINT PK_LAPTOP PRIMARY KEY (
    CODE
  )
  USING INDEX
    STORAGE (
      NEXT       1024 K
    )
/

PROMPT ALTER TABLE OUTCOME ADD CONSTRAINT PK_OUTCOME PRIMARY KEY
ALTER TABLE OUTCOME
  ADD CONSTRAINT PK_OUTCOME PRIMARY KEY (
    CODE
  )
  USING INDEX
    STORAGE (
      NEXT       1024 K
    )
/

PROMPT ALTER TABLE OUTCOME_O ADD CONSTRAINT PK_OUTCOME_O PRIMARY KEY
ALTER TABLE OUTCOME_O
  ADD CONSTRAINT PK_OUTCOME_O PRIMARY KEY (
    POINT,
    "date"
  )
  USING INDEX
    STORAGE (
      NEXT       1024 K
    )
/

PROMPT ALTER TABLE OUTCOMES ADD CONSTRAINT PK_OUTCOMES PRIMARY KEY
ALTER TABLE OUTCOMES
  ADD CONSTRAINT PK_OUTCOMES PRIMARY KEY (
    SHIP,
    BATTLE
  )
  USING INDEX
    STORAGE (
      NEXT       1024 K
    )
/

PROMPT ALTER TABLE PASS_IN_TRIP ADD CONSTRAINT PK_PT PRIMARY KEY
ALTER TABLE PASS_IN_TRIP
  ADD CONSTRAINT PK_PT PRIMARY KEY (
    TRIP_NO,
    "date",
    ID_PSG
  )
  USING INDEX
    STORAGE (
      NEXT       1024 K
    )
/

PROMPT ALTER TABLE PASSENGER ADD CONSTRAINT PK_PSG PRIMARY KEY
ALTER TABLE PASSENGER
  ADD CONSTRAINT PK_PSG PRIMARY KEY (
    ID_PSG
  )
  USING INDEX
    STORAGE (
      NEXT       1024 K
    )
/

PROMPT ALTER TABLE PC ADD CONSTRAINT PK_PC PRIMARY KEY
ALTER TABLE PC
  ADD CONSTRAINT PK_PC PRIMARY KEY (
    CODE
  )
  USING INDEX
    STORAGE (
      NEXT       1024 K
    )
/

PROMPT ALTER TABLE PRINTER ADD CONSTRAINT PK_PRINTER PRIMARY KEY
ALTER TABLE PRINTER
  ADD CONSTRAINT PK_PRINTER PRIMARY KEY (
    CODE
  )
  USING INDEX
    STORAGE (
      NEXT       1024 K
    )
/

PROMPT ALTER TABLE PRODUCT ADD CONSTRAINT PK_PRODUCT PRIMARY KEY
ALTER TABLE PRODUCT
  ADD CONSTRAINT PK_PRODUCT PRIMARY KEY (
    MODEL
  )
  USING INDEX
    STORAGE (
      NEXT       1024 K
    )
/

PROMPT ALTER TABLE SHIPS ADD CONSTRAINT PK_SHIPS PRIMARY KEY
ALTER TABLE SHIPS
  ADD CONSTRAINT PK_SHIPS PRIMARY KEY (
    NAME
  )
  USING INDEX
    STORAGE (
      NEXT       1024 K
    )
/

PROMPT ALTER TABLE TRIP ADD CONSTRAINT PK_T PRIMARY KEY
ALTER TABLE TRIP
  ADD CONSTRAINT PK_T PRIMARY KEY (
    TRIP_NO
  )
  USING INDEX
    STORAGE (
      NEXT       1024 K
    )
/

PROMPT ALTER TABLE UTB ADD CONSTRAINT PK_UTB PRIMARY KEY
ALTER TABLE UTB
  ADD CONSTRAINT PK_UTB PRIMARY KEY (
    B_DATETIME,
    B_Q_ID,
    B_V_ID
  )
  USING INDEX
    STORAGE (
      NEXT       1024 K
    )
/

PROMPT ALTER TABLE UTQ ADD CONSTRAINT PK_UTQ PRIMARY KEY
ALTER TABLE UTQ
  ADD CONSTRAINT PK_UTQ PRIMARY KEY (
    Q_ID
  )
  USING INDEX
    STORAGE (
      NEXT       1024 K
    )
/

PROMPT ALTER TABLE UTV ADD CONSTRAINT PK_UTV PRIMARY KEY
ALTER TABLE UTV
  ADD CONSTRAINT PK_UTV PRIMARY KEY (
    V_ID
  )
  USING INDEX
    STORAGE (
      NEXT       1024 K
    )
/

PROMPT =========================================================================
PROMPT FOREIGN CONSTRAINTS
PROMPT =========================================================================

PROMPT ALTER TABLE LAPTOP ADD CONSTRAINT FK_LAPTOP_PRODUCT FOREIGN KEY
ALTER TABLE LAPTOP
  ADD CONSTRAINT FK_LAPTOP_PRODUCT FOREIGN KEY (
    MODEL
  ) REFERENCES PRODUCT (
    MODEL
  )
/

PROMPT ALTER TABLE OUTCOMES ADD CONSTRAINT FK_OUTCOMES_BATTLES FOREIGN KEY
ALTER TABLE OUTCOMES
  ADD CONSTRAINT FK_OUTCOMES_BATTLES FOREIGN KEY (
    BATTLE
  ) REFERENCES BATTLES (
    NAME
  )
/

PROMPT ALTER TABLE PASS_IN_TRIP ADD CONSTRAINT FK_PASS_IN_TRIP_PASSENGER FOREIGN KEY
ALTER TABLE PASS_IN_TRIP
  ADD CONSTRAINT FK_PASS_IN_TRIP_PASSENGER FOREIGN KEY (
    ID_PSG
  ) REFERENCES PASSENGER (
    ID_PSG
  )
/

PROMPT ALTER TABLE PASS_IN_TRIP ADD CONSTRAINT FK_PASS_IN_TRIP_TRIP FOREIGN KEY
ALTER TABLE PASS_IN_TRIP
  ADD CONSTRAINT FK_PASS_IN_TRIP_TRIP FOREIGN KEY (
    TRIP_NO
  ) REFERENCES TRIP (
    TRIP_NO
  )
/

PROMPT ALTER TABLE PC ADD CONSTRAINT FK_PC_PRODUCT FOREIGN KEY
ALTER TABLE PC
  ADD CONSTRAINT FK_PC_PRODUCT FOREIGN KEY (
    MODEL
  ) REFERENCES PRODUCT (
    MODEL
  )
/

PROMPT ALTER TABLE PRINTER ADD CONSTRAINT FK_PRINTER_PRODUCT FOREIGN KEY
ALTER TABLE PRINTER
  ADD CONSTRAINT FK_PRINTER_PRODUCT FOREIGN KEY (
    MODEL
  ) REFERENCES PRODUCT (
    MODEL
  )
/

PROMPT ALTER TABLE SHIPS ADD CONSTRAINT FK_SHIPS_CLASSES FOREIGN KEY
ALTER TABLE SHIPS
  ADD CONSTRAINT FK_SHIPS_CLASSES FOREIGN KEY (
    CLASS
  ) REFERENCES CLASSES (
    CLASS
  )
/

PROMPT ALTER TABLE TRIP ADD CONSTRAINT FK_TRIP_COMPANY FOREIGN KEY
ALTER TABLE TRIP
  ADD CONSTRAINT FK_TRIP_COMPANY FOREIGN KEY (
    ID_COMP
  ) REFERENCES COMPANY (
    ID_COMP
  )
/

PROMPT ALTER TABLE UTB ADD CONSTRAINT FK_UTB_UTQ FOREIGN KEY
ALTER TABLE UTB
  ADD CONSTRAINT FK_UTB_UTQ FOREIGN KEY (
    B_Q_ID
  ) REFERENCES UTQ (
    Q_ID
  )
/

PROMPT ALTER TABLE UTB ADD CONSTRAINT FK_UTB_UTV FOREIGN KEY
ALTER TABLE UTB
  ADD CONSTRAINT FK_UTB_UTV FOREIGN KEY (
    B_V_ID
  ) REFERENCES UTV (
    V_ID
  )
/

PROMPT DATA battles

INSERT INTO battles (name, "date") VALUES ('Guadalcanal', timestamp'1942-11-15 00:00:00');
INSERT INTO battles (name, "date") VALUES ('North Atlantic', timestamp'1941-05-25 00:00:00');
INSERT INTO battles (name, "date") VALUES ('North Cape', timestamp'1943-12-26 00:00:00');
INSERT INTO battles (name, "date") VALUES ('Surigao Strait', timestamp'1944-10-25 00:00:00');
INSERT INTO battles (name, "date") VALUES ('#Cuba62a', timestamp'1962-10-20 00:00:00');
INSERT INTO battles (name, "date") VALUES ('#Cuba62b', timestamp'1962-10-25 00:00:00');

PROMPT DATA classes

INSERT INTO classes (class, type, country, numguns, bore, displacement) VALUES ('Bismarck', 'bb', 'Germany', 8, 15, 42000);
INSERT INTO classes (class, type, country, numguns, bore, displacement) VALUES ('Iowa', 'bb', 'USA', 9, 16, 46000);
INSERT INTO classes (class, type, country, numguns, bore, displacement) VALUES ('Kongo', 'bc', 'Japan', 8, 14, 32000);
INSERT INTO classes (class, type, country, numguns, bore, displacement) VALUES ('North Carolina', 'bb', 'USA', 12, 16, 37000);
INSERT INTO classes (class, type, country, numguns, bore, displacement) VALUES ('Renown', 'bc', 'Gt.Britain', 6, 15, 32000);
INSERT INTO classes (class, type, country, numguns, bore, displacement) VALUES ('Revenge', 'bb', 'Gt.Britain', 8, 15, 29000);
INSERT INTO classes (class, type, country, numguns, bore, displacement) VALUES ('Tennessee', 'bb', 'USA', 12, 14, 32000);
INSERT INTO classes (class, type, country, numguns, bore, displacement) VALUES ('Yamato', 'bb', 'Japan', 9, 18, 65000);

PROMPT DATA company

INSERT INTO company (id_comp, name) VALUES (1, 'Don_avia  ');
INSERT INTO company (id_comp, name) VALUES (2, 'Aeroflot  ');
INSERT INTO company (id_comp, name) VALUES (3, 'Dale_avia ');
INSERT INTO company (id_comp, name) VALUES (4, 'air_France');
INSERT INTO company (id_comp, name) VALUES (5, 'British_AW');


PROMPT DATA income

INSERT INTO income (code, point, "date", inc) VALUES (1, 1, timestamp'2001-03-22 00:00:00', 15000.00);
INSERT INTO income (code, point, "date", inc) VALUES (2, 1, timestamp'2001-03-23 00:00:00', 15000.00);
INSERT INTO income (code, point, "date", inc) VALUES (3, 1, timestamp'2001-03-24 00:00:00', 3600.00);
INSERT INTO income (code, point, "date", inc) VALUES (4, 2, timestamp'2001-03-22 00:00:00', 10000.00);
INSERT INTO income (code, point, "date", inc) VALUES (5, 2, timestamp'2001-03-24 00:00:00', 1500.00);
INSERT INTO income (code, point, "date", inc) VALUES (6, 1, timestamp'2001-04-13 00:00:00', 5000.00);
INSERT INTO income (code, point, "date", inc) VALUES (7, 1, timestamp'2001-05-11 00:00:00', 4500.00);
INSERT INTO income (code, point, "date", inc) VALUES (8, 1, timestamp'2001-03-22 00:00:00', 15000.00);
INSERT INTO income (code, point, "date", inc) VALUES (9, 2, timestamp'2001-03-24 00:00:00', 1500.00);
INSERT INTO income (code, point, "date", inc) VALUES (10, 1, timestamp'2001-04-13 00:00:00', 5000.00);
INSERT INTO income (code, point, "date", inc) VALUES (11, 1, timestamp'2001-03-24 00:00:00', 3400.00);
INSERT INTO income (code, point, "date", inc) VALUES (12, 3, timestamp'2001-09-13 00:00:00', 1350.00);
INSERT INTO income (code, point, "date", inc) VALUES (13, 3, timestamp'2001-09-13 00:00:00', 1750.00);


PROMPT DATA income_o

INSERT INTO income_o (point, "date", inc) VALUES (1, timestamp'2001-03-22 00:00:00', 15000.00);
INSERT INTO income_o (point, "date", inc) VALUES (1, timestamp'2001-03-23 00:00:00', 15000.00);
INSERT INTO income_o (point, "date", inc) VALUES (1, timestamp'2001-03-24 00:00:00', 3400.00);
INSERT INTO income_o (point, "date", inc) VALUES (1, timestamp'2001-04-13 00:00:00', 5000.00);
INSERT INTO income_o (point, "date", inc) VALUES (1, timestamp'2001-05-11 00:00:00', 4500.00);
INSERT INTO income_o (point, "date", inc) VALUES (2, timestamp'2001-03-22 00:00:00', 10000.00);
INSERT INTO income_o (point, "date", inc) VALUES (2, timestamp'2001-03-24 00:00:00', 1500.00);
INSERT INTO income_o (point, "date", inc) VALUES (3, timestamp'2001-09-13 00:00:00', 11500.00);
INSERT INTO income_o (point, "date", inc) VALUES (3, timestamp'2001-10-02 00:00:00', 18000.00);


PROMPT DATA product

INSERT INTO product (maker, model, type) VALUES ('B', '1121', 'PC');
INSERT INTO product (maker, model, type) VALUES ('A', '1232', 'PC');
INSERT INTO product (maker, model, type) VALUES ('A', '1233', 'PC');
INSERT INTO product (maker, model, type) VALUES ('E', '1260', 'PC');
INSERT INTO product (maker, model, type) VALUES ('A', '1276', 'Printer');
INSERT INTO product (maker, model, type) VALUES ('D', '1288', 'Printer');
INSERT INTO product (maker, model, type) VALUES ('A', '1298', 'Laptop');
INSERT INTO product (maker, model, type) VALUES ('C', '1321', 'Laptop');
INSERT INTO product (maker, model, type) VALUES ('A', '1401', 'Printer');
INSERT INTO product (maker, model, type) VALUES ('A', '1408', 'Printer');
INSERT INTO product (maker, model, type) VALUES ('D', '1433', 'Printer');
INSERT INTO product (maker, model, type) VALUES ('E', '1434', 'Printer');
INSERT INTO product (maker, model, type) VALUES ('B', '1750', 'Laptop');
INSERT INTO product (maker, model, type) VALUES ('A', '1752', 'Laptop');
INSERT INTO product (maker, model, type) VALUES ('E', '2113', 'PC');
INSERT INTO product (maker, model, type) VALUES ('E', '2112', 'PC');

PROMPT DATA laptop

INSERT INTO laptop (code, model, speed, ram, hd, price, screen) VALUES (1, '1298', 350, 32, 4, 700.00, 11);
INSERT INTO laptop (code, model, speed, ram, hd, price, screen) VALUES (2, '1321', 500, 64, 8, 970.00, 12);
INSERT INTO laptop (code, model, speed, ram, hd, price, screen) VALUES (3, '1750', 750, 128, 12, 1200.00, 14);
INSERT INTO laptop (code, model, speed, ram, hd, price, screen) VALUES (4, '1298', 600, 64, 10, 1050.00, 15);
INSERT INTO laptop (code, model, speed, ram, hd, price, screen) VALUES (5, '1752', 750, 128, 10, 1150.00, 14);
INSERT INTO laptop (code, model, speed, ram, hd, price, screen) VALUES (6, '1298', 450, 64, 10, 950.00, 12);


PROMPT DATA outcome

INSERT INTO outcome (code, point, "date", "out") VALUES (1, 1, timestamp'2001-03-14 00:00:00', 15348.00);
INSERT INTO outcome (code, point, "date", "out") VALUES (2, 1, timestamp'2001-03-24 00:00:00', 3663.00);
INSERT INTO outcome (code, point, "date", "out") VALUES (3, 1, timestamp'2001-03-26 00:00:00', 1221.00);
INSERT INTO outcome (code, point, "date", "out") VALUES (4, 1, timestamp'2001-03-28 00:00:00', 2075.00);
INSERT INTO outcome (code, point, "date", "out") VALUES (5, 1, timestamp'2001-03-29 00:00:00', 2004.00);
INSERT INTO outcome (code, point, "date", "out") VALUES (6, 1, timestamp'2001-04-11 00:00:00', 3195.04);
INSERT INTO outcome (code, point, "date", "out") VALUES (7, 1, timestamp'2001-04-13 00:00:00', 4490.00);
INSERT INTO outcome (code, point, "date", "out") VALUES (8, 1, timestamp'2001-04-27 00:00:00', 3110.00);
INSERT INTO outcome (code, point, "date", "out") VALUES (9, 1, timestamp'2001-05-11 00:00:00', 2530.00);
INSERT INTO outcome (code, point, "date", "out") VALUES (10, 2, timestamp'2001-03-22 00:00:00', 1440.00);
INSERT INTO outcome (code, point, "date", "out") VALUES (11, 2, timestamp'2001-03-29 00:00:00', 7848.00);
INSERT INTO outcome (code, point, "date", "out") VALUES (12, 2, timestamp'2001-04-02 00:00:00', 2040.00);
INSERT INTO outcome (code, point, "date", "out") VALUES (13, 1, timestamp'2001-03-24 00:00:00', 3500.00);
INSERT INTO outcome (code, point, "date", "out") VALUES (14, 2, timestamp'2001-03-22 00:00:00', 1440.00);
INSERT INTO outcome (code, point, "date", "out") VALUES (15, 1, timestamp'2001-03-29 00:00:00', 2006.00);
INSERT INTO outcome (code, point, "date", "out") VALUES (16, 3, timestamp'2001-09-13 00:00:00', 1200.00);
INSERT INTO outcome (code, point, "date", "out") VALUES (17, 3, timestamp'2001-09-13 00:00:00', 1500.00);
INSERT INTO outcome (code, point, "date", "out") VALUES (18, 3, timestamp'2001-09-14 00:00:00', 1150.00);


PROMPT DATA outcome_o

INSERT INTO outcome_o (point, "date", "out") VALUES (1, timestamp'2001-03-14 00:00:00', 15348.00);
INSERT INTO outcome_o (point, "date", "out") VALUES (1, timestamp'2001-03-24 00:00:00', 3663.00);
INSERT INTO outcome_o (point, "date", "out") VALUES (1, timestamp'2001-03-26 00:00:00', 1221.00);
INSERT INTO outcome_o (point, "date", "out") VALUES (1, timestamp'2001-03-28 00:00:00', 2075.00);
INSERT INTO outcome_o (point, "date", "out") VALUES (1, timestamp'2001-03-29 00:00:00', 2004.00);
INSERT INTO outcome_o (point, "date", "out") VALUES (1, timestamp'2001-04-11 00:00:00', 3195.04);
INSERT INTO outcome_o (point, "date", "out") VALUES (1, timestamp'2001-04-13 00:00:00', 4490.00);
INSERT INTO outcome_o (point, "date", "out") VALUES (1, timestamp'2001-04-27 00:00:00', 3110.00);
INSERT INTO outcome_o (point, "date", "out") VALUES (1, timestamp'2001-05-11 00:00:00', 2530.00);
INSERT INTO outcome_o (point, "date", "out") VALUES (2, timestamp'2001-03-22 00:00:00', 1440.00);
INSERT INTO outcome_o (point, "date", "out") VALUES (2, timestamp'2001-03-29 00:00:00', 7848.00);
INSERT INTO outcome_o (point, "date", "out") VALUES (2, timestamp'2001-04-02 00:00:00', 2040.00);
INSERT INTO outcome_o (point, "date", "out") VALUES (3, timestamp'2001-09-13 00:00:00', 1500.00);
INSERT INTO outcome_o (point, "date", "out") VALUES (3, timestamp'2001-09-14 00:00:00', 2300.00);
INSERT INTO outcome_o (point, "date", "out") VALUES (3, timestamp'2002-09-16 00:00:00', 2150.00);


PROMPT DATA outcomes

INSERT INTO outcomes (ship, battle, result) VALUES ('Bismarck', 'North Atlantic', 'sunk');
INSERT INTO outcomes (ship, battle, result) VALUES ('California', 'Surigao Strait', 'OK');
INSERT INTO outcomes (ship, battle, result) VALUES ('Duke of York', 'North Cape', 'OK');
INSERT INTO outcomes (ship, battle, result) VALUES ('Fuso', 'Surigao Strait', 'sunk');
INSERT INTO outcomes (ship, battle, result) VALUES ('Hood', 'North Atlantic', 'sunk');
INSERT INTO outcomes (ship, battle, result) VALUES ('King George V', 'North Atlantic', 'OK');
INSERT INTO outcomes (ship, battle, result) VALUES ('Kirishima', 'Guadalcanal', 'sunk');
INSERT INTO outcomes (ship, battle, result) VALUES ('Prince of Wales', 'North Atlantic', 'damaged');
INSERT INTO outcomes (ship, battle, result) VALUES ('Rodney', 'North Atlantic', 'OK');
INSERT INTO outcomes (ship, battle, result) VALUES ('Schamhorst', 'North Cape', 'sunk');
INSERT INTO outcomes (ship, battle, result) VALUES ('South Dakota', 'Guadalcanal', 'damaged');
INSERT INTO outcomes (ship, battle, result) VALUES ('Tennessee', 'Surigao Strait', 'OK');
INSERT INTO outcomes (ship, battle, result) VALUES ('Washington', 'Guadalcanal', 'OK');
INSERT INTO outcomes (ship, battle, result) VALUES ('West Virginia', 'Surigao Strait', 'OK');
INSERT INTO outcomes (ship, battle, result) VALUES ('Yamashiro', 'Surigao Strait', 'sunk');
INSERT INTO outcomes (ship, battle, result) VALUES ('California', 'Guadalcanal', 'damaged');

PROMPT DATA trip

INSERT INTO trip (trip_no, id_comp, plane, town_from, town_to, time_out, time_in) VALUES (1100, 4, 'Boeing    ', 'Rostov                   ', 'Paris                    ', timestamp'1900-01-01 14:30:00', timestamp'1900-01-01 17:50:00');
INSERT INTO trip (trip_no, id_comp, plane, town_from, town_to, time_out, time_in) VALUES (1101, 4, 'Boeing    ', 'Paris                    ', 'Rostov                   ', timestamp'1900-01-01 08:12:00', timestamp'1900-01-01 11:45:00');
INSERT INTO trip (trip_no, id_comp, plane, town_from, town_to, time_out, time_in) VALUES (1123, 3, 'TU-154    ', 'Rostov                   ', 'Vladivostok              ', timestamp'1900-01-01 16:20:00', timestamp'1900-01-01 03:40:00');
INSERT INTO trip (trip_no, id_comp, plane, town_from, town_to, time_out, time_in) VALUES (1124, 3, 'TU-154    ', 'Vladivostok              ', 'Rostov                   ', timestamp'1900-01-01 09:00:00', timestamp'1900-01-01 19:50:00');
INSERT INTO trip (trip_no, id_comp, plane, town_from, town_to, time_out, time_in) VALUES (1145, 2, 'IL-86     ', 'Moscow                   ', 'Rostov                   ', timestamp'1900-01-01 09:35:00', timestamp'1900-01-01 11:23:00');
INSERT INTO trip (trip_no, id_comp, plane, town_from, town_to, time_out, time_in) VALUES (1146, 2, 'IL-86     ', 'Rostov                   ', 'Moscow                   ', timestamp'1900-01-01 17:55:00', timestamp'1900-01-01 20:01:00');
INSERT INTO trip (trip_no, id_comp, plane, town_from, town_to, time_out, time_in) VALUES (1181, 1, 'TU-134    ', 'Rostov                   ', 'Moscow                   ', timestamp'1900-01-01 06:12:00', timestamp'1900-01-01 08:01:00');
INSERT INTO trip (trip_no, id_comp, plane, town_from, town_to, time_out, time_in) VALUES (1182, 1, 'TU-134    ', 'Moscow                   ', 'Rostov                   ', timestamp'1900-01-01 12:35:00', timestamp'1900-01-01 14:30:00');
INSERT INTO trip (trip_no, id_comp, plane, town_from, town_to, time_out, time_in) VALUES (1187, 1, 'TU-134    ', 'Rostov                   ', 'Moscow                   ', timestamp'1900-01-01 15:42:00', timestamp'1900-01-01 17:39:00');
INSERT INTO trip (trip_no, id_comp, plane, town_from, town_to, time_out, time_in) VALUES (1188, 1, 'TU-134    ', 'Moscow                   ', 'Rostov                   ', timestamp'1900-01-01 22:50:00', timestamp'1900-01-01 00:48:00');
INSERT INTO trip (trip_no, id_comp, plane, town_from, town_to, time_out, time_in) VALUES (1195, 1, 'TU-154    ', 'Rostov                   ', 'Moscow                   ', timestamp'1900-01-01 23:30:00', timestamp'1900-01-01 01:11:00');
INSERT INTO trip (trip_no, id_comp, plane, town_from, town_to, time_out, time_in) VALUES (1196, 1, 'TU-154    ', 'Moscow                   ', 'Rostov                   ', timestamp'1900-01-01 04:00:00', timestamp'1900-01-01 05:45:00');
INSERT INTO trip (trip_no, id_comp, plane, town_from, town_to, time_out, time_in) VALUES (7771, 5, 'Boeing    ', 'London                   ', 'Singapore                ', timestamp'1900-01-01 01:00:00', timestamp'1900-01-01 11:00:00');
INSERT INTO trip (trip_no, id_comp, plane, town_from, town_to, time_out, time_in) VALUES (7772, 5, 'Boeing    ', 'Singapore                ', 'London                   ', timestamp'1900-01-01 12:00:00', timestamp'1900-01-01 02:00:00');
INSERT INTO trip (trip_no, id_comp, plane, town_from, town_to, time_out, time_in) VALUES (7773, 5, 'Boeing    ', 'London                   ', 'Singapore                ', timestamp'1900-01-01 03:00:00', timestamp'1900-01-01 13:00:00');
INSERT INTO trip (trip_no, id_comp, plane, town_from, town_to, time_out, time_in) VALUES (7774, 5, 'Boeing    ', 'Singapore                ', 'London                   ', timestamp'1900-01-01 14:00:00', timestamp'1900-01-01 06:00:00');
INSERT INTO trip (trip_no, id_comp, plane, town_from, town_to, time_out, time_in) VALUES (7775, 5, 'Boeing    ', 'London                   ', 'Singapore                ', timestamp'1900-01-01 09:00:00', timestamp'1900-01-01 20:00:00');
INSERT INTO trip (trip_no, id_comp, plane, town_from, town_to, time_out, time_in) VALUES (7776, 5, 'Boeing    ', 'Singapore                ', 'London                   ', timestamp'1900-01-01 18:00:00', timestamp'1900-01-01 08:00:00');
INSERT INTO trip (trip_no, id_comp, plane, town_from, town_to, time_out, time_in) VALUES (7777, 5, 'Boeing    ', 'London                   ', 'Singapore                ', timestamp'1900-01-01 18:00:00', timestamp'1900-01-01 06:00:00');
INSERT INTO trip (trip_no, id_comp, plane, town_from, town_to, time_out, time_in) VALUES (7778, 5, 'Boeing    ', 'Singapore                ', 'London                   ', timestamp'1900-01-01 22:00:00', timestamp'1900-01-01 12:00:00');
INSERT INTO trip (trip_no, id_comp, plane, town_from, town_to, time_out, time_in) VALUES (8881, 5, 'Boeing    ', 'London                   ', 'Paris                    ', timestamp'1900-01-01 03:00:00', timestamp'1900-01-01 04:00:00');
INSERT INTO trip (trip_no, id_comp, plane, town_from, town_to, time_out, time_in) VALUES (8882, 5, 'Boeing    ', 'Paris                    ', 'London                   ', timestamp'1900-01-01 22:00:00', timestamp'1900-01-01 23:00:00');

PROMPT DATA passenger

INSERT INTO passenger (id_psg, name) VALUES (1, 'Bruce Willis        ');
INSERT INTO passenger (id_psg, name) VALUES (2, 'George Clooney      ');
INSERT INTO passenger (id_psg, name) VALUES (3, 'Kevin Costner       ');
INSERT INTO passenger (id_psg, name) VALUES (4, 'Donald Sutherland   ');
INSERT INTO passenger (id_psg, name) VALUES (5, 'Jennifer Lopez      ');
INSERT INTO passenger (id_psg, name) VALUES (6, 'Ray Liotta          ');
INSERT INTO passenger (id_psg, name) VALUES (7, 'Samuel L. Jackson   ');
INSERT INTO passenger (id_psg, name) VALUES (8, 'Nikole Kidman       ');
INSERT INTO passenger (id_psg, name) VALUES (9, 'Alan Rickman        ');
INSERT INTO passenger (id_psg, name) VALUES (10, 'Kurt Russell        ');
INSERT INTO passenger (id_psg, name) VALUES (11, 'Harrison Ford       ');
INSERT INTO passenger (id_psg, name) VALUES (12, 'Russell Crowe       ');
INSERT INTO passenger (id_psg, name) VALUES (13, 'Steve Martin        ');
INSERT INTO passenger (id_psg, name) VALUES (14, 'Michael Caine       ');
INSERT INTO passenger (id_psg, name) VALUES (15, 'Angelina Jolie      ');
INSERT INTO passenger (id_psg, name) VALUES (16, 'Mel Gibson          ');
INSERT INTO passenger (id_psg, name) VALUES (17, 'Michael Douglas     ');
INSERT INTO passenger (id_psg, name) VALUES (18, 'John Travolta       ');
INSERT INTO passenger (id_psg, name) VALUES (19, 'Sylvester Stallone  ');
INSERT INTO passenger (id_psg, name) VALUES (20, 'Tommy Lee Jones     ');
INSERT INTO passenger (id_psg, name) VALUES (21, 'Catherine Zeta-Jones');
INSERT INTO passenger (id_psg, name) VALUES (22, 'Antonio Banderas    ');
INSERT INTO passenger (id_psg, name) VALUES (23, 'Kim Basinger        ');
INSERT INTO passenger (id_psg, name) VALUES (24, 'Sam Neill           ');
INSERT INTO passenger (id_psg, name) VALUES (25, 'Gary Oldman         ');
INSERT INTO passenger (id_psg, name) VALUES (26, 'Clint Eastwood      ');
INSERT INTO passenger (id_psg, name) VALUES (27, 'Brad Pitt           ');
INSERT INTO passenger (id_psg, name) VALUES (28, 'Johnny Depp         ');
INSERT INTO passenger (id_psg, name) VALUES (29, 'Pierce Brosnan      ');
INSERT INTO passenger (id_psg, name) VALUES (30, 'Sean Connery        ');
INSERT INTO passenger (id_psg, name) VALUES (31, 'Bruce Willis        ');
INSERT INTO passenger (id_psg, name) VALUES (37, 'Mullah Omar         ');

PROMPT DATA pass_in_trip

INSERT INTO pass_in_trip (trip_no, "date", id_psg, place) VALUES (1100, timestamp'2003-04-29 00:00:00', 1, '1a        ');
INSERT INTO pass_in_trip (trip_no, "date", id_psg, place) VALUES (1123, timestamp'2003-04-05 00:00:00', 3, '2a        ');
INSERT INTO pass_in_trip (trip_no, "date", id_psg, place) VALUES (1123, timestamp'2003-04-08 00:00:00', 1, '4c        ');
INSERT INTO pass_in_trip (trip_no, "date", id_psg, place) VALUES (1123, timestamp'2003-04-08 00:00:00', 6, '4b        ');
INSERT INTO pass_in_trip (trip_no, "date", id_psg, place) VALUES (1124, timestamp'2003-04-02 00:00:00', 2, '2d        ');
INSERT INTO pass_in_trip (trip_no, "date", id_psg, place) VALUES (1145, timestamp'2003-04-05 00:00:00', 3, '2c        ');
INSERT INTO pass_in_trip (trip_no, "date", id_psg, place) VALUES (1181, timestamp'2003-04-01 00:00:00', 1, '1a        ');
INSERT INTO pass_in_trip (trip_no, "date", id_psg, place) VALUES (1181, timestamp'2003-04-01 00:00:00', 6, '1b        ');
INSERT INTO pass_in_trip (trip_no, "date", id_psg, place) VALUES (1181, timestamp'2003-04-01 00:00:00', 8, '3c        ');
INSERT INTO pass_in_trip (trip_no, "date", id_psg, place) VALUES (1181, timestamp'2003-04-13 00:00:00', 5, '1b        ');
INSERT INTO pass_in_trip (trip_no, "date", id_psg, place) VALUES (1182, timestamp'2003-04-13 00:00:00', 5, '4b        ');
INSERT INTO pass_in_trip (trip_no, "date", id_psg, place) VALUES (1187, timestamp'2003-04-14 00:00:00', 8, '3a        ');
INSERT INTO pass_in_trip (trip_no, "date", id_psg, place) VALUES (1188, timestamp'2003-04-01 00:00:00', 8, '3a        ');
INSERT INTO pass_in_trip (trip_no, "date", id_psg, place) VALUES (1182, timestamp'2003-04-13 00:00:00', 9, '6d        ');
INSERT INTO pass_in_trip (trip_no, "date", id_psg, place) VALUES (1145, timestamp'2003-04-25 00:00:00', 5, '1d        ');
INSERT INTO pass_in_trip (trip_no, "date", id_psg, place) VALUES (1187, timestamp'2003-04-14 00:00:00', 10, '3d        ');
INSERT INTO pass_in_trip (trip_no, "date", id_psg, place) VALUES (8882, timestamp'2005-11-06 00:00:00', 37, '1a        ');
INSERT INTO pass_in_trip (trip_no, "date", id_psg, place) VALUES (7771, timestamp'2005-11-07 00:00:00', 37, '1c        ');
INSERT INTO pass_in_trip (trip_no, "date", id_psg, place) VALUES (7772, timestamp'2005-11-07 00:00:00', 37, '1a        ');
INSERT INTO pass_in_trip (trip_no, "date", id_psg, place) VALUES (8881, timestamp'2005-11-08 00:00:00', 37, '1d        ');
INSERT INTO pass_in_trip (trip_no, "date", id_psg, place) VALUES (7778, timestamp'2005-11-05 00:00:00', 10, '2a        ');
INSERT INTO pass_in_trip (trip_no, "date", id_psg, place) VALUES (7772, timestamp'2005-11-29 00:00:00', 10, '3a        ');
INSERT INTO pass_in_trip (trip_no, "date", id_psg, place) VALUES (7771, timestamp'2005-11-04 00:00:00', 11, '4a        ');
INSERT INTO pass_in_trip (trip_no, "date", id_psg, place) VALUES (7771, timestamp'2005-11-07 00:00:00', 11, '1b        ');
INSERT INTO pass_in_trip (trip_no, "date", id_psg, place) VALUES (7771, timestamp'2005-11-09 00:00:00', 11, '5a        ');
INSERT INTO pass_in_trip (trip_no, "date", id_psg, place) VALUES (7772, timestamp'2005-11-07 00:00:00', 12, '1d        ');
INSERT INTO pass_in_trip (trip_no, "date", id_psg, place) VALUES (7773, timestamp'2005-11-07 00:00:00', 13, '2d        ');
INSERT INTO pass_in_trip (trip_no, "date", id_psg, place) VALUES (7772, timestamp'2005-11-29 00:00:00', 13, '1b        ');
INSERT INTO pass_in_trip (trip_no, "date", id_psg, place) VALUES (8882, timestamp'2005-11-13 00:00:00', 14, '3d        ');
INSERT INTO pass_in_trip (trip_no, "date", id_psg, place) VALUES (7771, timestamp'2005-11-14 00:00:00', 14, '4d        ');
INSERT INTO pass_in_trip (trip_no, "date", id_psg, place) VALUES (7771, timestamp'2005-11-16 00:00:00', 14, '5d        ');
INSERT INTO pass_in_trip (trip_no, "date", id_psg, place) VALUES (7772, timestamp'2005-11-29 00:00:00', 14, '1c        ');


PROMPT DATA pc

INSERT INTO pc (code, model, speed, ram, hd, cd, price) VALUES (1, '1232', 500, 64, 5, '12x', 600.00);
INSERT INTO pc (code, model, speed, ram, hd, cd, price) VALUES (2, '1121', 750, 128, 14, '40x', 850.00);
INSERT INTO pc (code, model, speed, ram, hd, cd, price) VALUES (3, '1233', 500, 64, 5, '12x', 600.00);
INSERT INTO pc (code, model, speed, ram, hd, cd, price) VALUES (4, '1121', 600, 128, 14, '40x', 850.00);
INSERT INTO pc (code, model, speed, ram, hd, cd, price) VALUES (5, '1121', 600, 128, 8, '40x', 850.00);
INSERT INTO pc (code, model, speed, ram, hd, cd, price) VALUES (6, '1233', 750, 128, 20, '50x', 950.00);
INSERT INTO pc (code, model, speed, ram, hd, cd, price) VALUES (7, '1232', 500, 32, 10, '12x', 400.00);
INSERT INTO pc (code, model, speed, ram, hd, cd, price) VALUES (8, '1232', 450, 64, 8, '24x', 350.00);
INSERT INTO pc (code, model, speed, ram, hd, cd, price) VALUES (9, '1232', 450, 32, 10, '24x', 350.00);
INSERT INTO pc (code, model, speed, ram, hd, cd, price) VALUES (10, '1260', 500, 32, 10, '12x', 350.00);
INSERT INTO pc (code, model, speed, ram, hd, cd, price) VALUES (11, '1233', 900, 128, 40, '40x', 980.00);
INSERT INTO pc (code, model, speed, ram, hd, cd, price) VALUES (12, '1233', 800, 128, 20, '50x', 970.00);

PROMPT DATA printer

INSERT INTO printer (code, model, color, type, price) VALUES (1, '1276', 'n', 'Laser', 400.00);
INSERT INTO printer (code, model, color, type, price) VALUES (2, '1433', 'y', 'Jet', 270.00);
INSERT INTO printer (code, model, color, type, price) VALUES (3, '1434', 'y', 'Jet', 290.00);
INSERT INTO printer (code, model, color, type, price) VALUES (4, '1401', 'n', 'Matrix', 150.00);
INSERT INTO printer (code, model, color, type, price) VALUES (5, '1408', 'n', 'Matrix', 270.00);
INSERT INTO printer (code, model, color, type, price) VALUES (6, '1288', 'n', 'Laser', 400.00);


PROMPT DATA ships

INSERT INTO ships (name, class, launched) VALUES ('California', 'Tennessee', 1921);
INSERT INTO ships (name, class, launched) VALUES ('Haruna', 'Kongo', 1916);
INSERT INTO ships (name, class, launched) VALUES ('Hiei', 'Kongo', 1914);
INSERT INTO ships (name, class, launched) VALUES ('Iowa', 'Iowa', 1943);
INSERT INTO ships (name, class, launched) VALUES ('Kirishima', 'Kongo', 1915);
INSERT INTO ships (name, class, launched) VALUES ('Kongo', 'Kongo', 1913);
INSERT INTO ships (name, class, launched) VALUES ('Missouri', 'Iowa', 1944);
INSERT INTO ships (name, class, launched) VALUES ('Musashi', 'Yamato', 1942);
INSERT INTO ships (name, class, launched) VALUES ('New Jersey', 'Iowa', 1943);
INSERT INTO ships (name, class, launched) VALUES ('North Carolina', 'North Carolina', 1941);
INSERT INTO ships (name, class, launched) VALUES ('Ramillies', 'Revenge', 1917);
INSERT INTO ships (name, class, launched) VALUES ('Renown', 'Renown', 1916);
INSERT INTO ships (name, class, launched) VALUES ('Repulse', 'Renown', 1916);
INSERT INTO ships (name, class, launched) VALUES ('Resolution', 'Renown', 1916);
INSERT INTO ships (name, class, launched) VALUES ('Revenge', 'Revenge', 1916);
INSERT INTO ships (name, class, launched) VALUES ('Royal Oak', 'Revenge', 1916);
INSERT INTO ships (name, class, launched) VALUES ('Royal Sovereign', 'Revenge', 1916);
INSERT INTO ships (name, class, launched) VALUES ('Tennessee', 'Tennessee', 1920);
INSERT INTO ships (name, class, launched) VALUES ('Washington', 'North Carolina', 1941);
INSERT INTO ships (name, class, launched) VALUES ('Wisconsin', 'Iowa', 1944);
INSERT INTO ships (name, class, launched) VALUES ('Yamato', 'Yamato', 1941);
INSERT INTO ships (name, class, launched) VALUES ('South Dakota', 'North Carolina', 1941);


PROMPT DATA utq

INSERT INTO utq (q_id, q_name) VALUES (1, 'Square # 01');
INSERT INTO utq (q_id, q_name) VALUES (2, 'Square # 02');
INSERT INTO utq (q_id, q_name) VALUES (3, 'Square # 03');
INSERT INTO utq (q_id, q_name) VALUES (4, 'Square # 04');
INSERT INTO utq (q_id, q_name) VALUES (5, 'Square # 05');
INSERT INTO utq (q_id, q_name) VALUES (6, 'Square # 06');
INSERT INTO utq (q_id, q_name) VALUES (7, 'Square # 07');
INSERT INTO utq (q_id, q_name) VALUES (8, 'Square # 08');
INSERT INTO utq (q_id, q_name) VALUES (9, 'Square # 09');
INSERT INTO utq (q_id, q_name) VALUES (10, 'Square # 10');
INSERT INTO utq (q_id, q_name) VALUES (11, 'Square # 11');
INSERT INTO utq (q_id, q_name) VALUES (12, 'Square # 12');
INSERT INTO utq (q_id, q_name) VALUES (13, 'Square # 13');
INSERT INTO utq (q_id, q_name) VALUES (14, 'Square # 14');
INSERT INTO utq (q_id, q_name) VALUES (15, 'Square # 15');
INSERT INTO utq (q_id, q_name) VALUES (16, 'Square # 16');
INSERT INTO utq (q_id, q_name) VALUES (17, 'Square # 17');
INSERT INTO utq (q_id, q_name) VALUES (18, 'Square # 18');
INSERT INTO utq (q_id, q_name) VALUES (19, 'Square # 19');
INSERT INTO utq (q_id, q_name) VALUES (20, 'Square # 20');
INSERT INTO utq (q_id, q_name) VALUES (21, 'Square # 21');
INSERT INTO utq (q_id, q_name) VALUES (22, 'Square # 22');
INSERT INTO utq (q_id, q_name) VALUES (23, 'Square # 23');
INSERT INTO utq (q_id, q_name) VALUES (25, 'Square # 25');

PROMPT DATA utv

INSERT INTO utv (v_id, v_name, v_color) VALUES (1, 'Balloon # 01', 'R');
INSERT INTO utv (v_id, v_name, v_color) VALUES (2, 'Balloon # 02', 'R');
INSERT INTO utv (v_id, v_name, v_color) VALUES (3, 'Balloon # 03', 'R');
INSERT INTO utv (v_id, v_name, v_color) VALUES (4, 'Balloon # 04', 'G');
INSERT INTO utv (v_id, v_name, v_color) VALUES (5, 'Balloon # 05', 'G');
INSERT INTO utv (v_id, v_name, v_color) VALUES (6, 'Balloon # 06', 'G');
INSERT INTO utv (v_id, v_name, v_color) VALUES (7, 'Balloon # 07', 'B');
INSERT INTO utv (v_id, v_name, v_color) VALUES (8, 'Balloon # 08', 'B');
INSERT INTO utv (v_id, v_name, v_color) VALUES (9, 'Balloon # 09', 'B');
INSERT INTO utv (v_id, v_name, v_color) VALUES (10, 'Balloon # 10', 'R');
INSERT INTO utv (v_id, v_name, v_color) VALUES (11, 'Balloon # 11', 'R');
INSERT INTO utv (v_id, v_name, v_color) VALUES (12, 'Balloon # 12', 'R');
INSERT INTO utv (v_id, v_name, v_color) VALUES (13, 'Balloon # 13', 'G');
INSERT INTO utv (v_id, v_name, v_color) VALUES (14, 'Balloon # 14', 'G');
INSERT INTO utv (v_id, v_name, v_color) VALUES (15, 'Balloon # 15', 'B');
INSERT INTO utv (v_id, v_name, v_color) VALUES (16, 'Balloon # 16', 'B');
INSERT INTO utv (v_id, v_name, v_color) VALUES (17, 'Balloon # 17', 'R');
INSERT INTO utv (v_id, v_name, v_color) VALUES (18, 'Balloon # 18', 'G');
INSERT INTO utv (v_id, v_name, v_color) VALUES (19, 'Balloon # 19', 'B');
INSERT INTO utv (v_id, v_name, v_color) VALUES (20, 'Balloon # 20', 'R');
INSERT INTO utv (v_id, v_name, v_color) VALUES (21, 'Balloon # 21', 'G');
INSERT INTO utv (v_id, v_name, v_color) VALUES (22, 'Balloon # 22', 'B');
INSERT INTO utv (v_id, v_name, v_color) VALUES (23, 'Balloon # 23', 'R');
INSERT INTO utv (v_id, v_name, v_color) VALUES (24, 'Balloon # 24', 'G');
INSERT INTO utv (v_id, v_name, v_color) VALUES (25, 'Balloon # 25', 'B');
INSERT INTO utv (v_id, v_name, v_color) VALUES (26, 'Balloon # 26', 'B');
INSERT INTO utv (v_id, v_name, v_color) VALUES (27, 'Balloon # 27', 'R');
INSERT INTO utv (v_id, v_name, v_color) VALUES (28, 'Balloon # 28', 'G');
INSERT INTO utv (v_id, v_name, v_color) VALUES (29, 'Balloon # 29', 'R');
INSERT INTO utv (v_id, v_name, v_color) VALUES (30, 'Balloon # 30', 'G');
INSERT INTO utv (v_id, v_name, v_color) VALUES (31, 'Balloon # 31', 'R');
INSERT INTO utv (v_id, v_name, v_color) VALUES (32, 'Balloon # 32', 'G');
INSERT INTO utv (v_id, v_name, v_color) VALUES (33, 'Balloon # 33', 'B');
INSERT INTO utv (v_id, v_name, v_color) VALUES (34, 'Balloon # 34', 'R');
INSERT INTO utv (v_id, v_name, v_color) VALUES (35, 'Balloon # 35', 'G');
INSERT INTO utv (v_id, v_name, v_color) VALUES (36, 'Balloon # 36', 'B');
INSERT INTO utv (v_id, v_name, v_color) VALUES (37, 'Balloon # 37', 'R');
INSERT INTO utv (v_id, v_name, v_color) VALUES (38, 'Balloon # 38', 'G');
INSERT INTO utv (v_id, v_name, v_color) VALUES (39, 'Balloon # 39', 'B');
INSERT INTO utv (v_id, v_name, v_color) VALUES (40, 'Balloon # 40', 'R');
INSERT INTO utv (v_id, v_name, v_color) VALUES (41, 'Balloon # 41', 'R');
INSERT INTO utv (v_id, v_name, v_color) VALUES (42, 'Balloon # 42', 'G');
INSERT INTO utv (v_id, v_name, v_color) VALUES (43, 'Balloon # 43', 'B');
INSERT INTO utv (v_id, v_name, v_color) VALUES (44, 'Balloon # 44', 'R');
INSERT INTO utv (v_id, v_name, v_color) VALUES (45, 'Balloon # 45', 'G');
INSERT INTO utv (v_id, v_name, v_color) VALUES (46, 'Balloon # 46', 'B');
INSERT INTO utv (v_id, v_name, v_color) VALUES (47, 'Balloon # 47', 'B');
INSERT INTO utv (v_id, v_name, v_color) VALUES (48, 'Balloon # 48', 'G');
INSERT INTO utv (v_id, v_name, v_color) VALUES (49, 'Balloon # 49', 'R');
INSERT INTO utv (v_id, v_name, v_color) VALUES (50, 'Balloon # 50', 'G');
INSERT INTO utv (v_id, v_name, v_color) VALUES (51, 'Balloon # 51', 'B');
INSERT INTO utv (v_id, v_name, v_color) VALUES (52, 'Balloon # 52', 'R');
INSERT INTO utv (v_id, v_name, v_color) VALUES (53, 'Balloon # 53', 'G');
INSERT INTO utv (v_id, v_name, v_color) VALUES (54, 'Balloon # 54', 'B');

PROMPT DATA utb

INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:12:01', 1, 1, 155);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-06-23 01:12:02', 1, 1, 100);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:12:03', 2, 2, 255);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:12:04', 3, 3, 255);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:12:05', 1, 4, 255);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:12:06', 2, 5, 255);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:12:07', 3, 6, 255);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:12:08', 1, 7, 255);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:12:09', 2, 8, 255);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:12:10', 3, 9, 255);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:12:11', 4, 10, 50);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:12:12', 5, 11, 100);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:12:13', 5, 12, 155);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:12:14', 5, 13, 155);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:12:15', 5, 14, 100);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:12:16', 5, 15, 50);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:12:17', 5, 16, 205);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:12:18', 6, 10, 155);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:12:19', 6, 17, 100);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:12:20', 6, 18, 255);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:12:21', 6, 19, 255);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:12:22', 7, 17, 155);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:12:23', 7, 20, 100);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:12:24', 7, 21, 255);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:12:25', 7, 22, 255);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:12:26', 8, 10, 50);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:12:27', 9, 23, 255);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:12:28', 9, 24, 255);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:12:29', 9, 25, 100);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:12:30', 9, 26, 155);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:12:31', 10, 25, 155);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:12:31', 10, 26, 100);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:12:33', 10, 27, 10);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:12:34', 10, 28, 10);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:12:35', 10, 29, 245);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:12:36', 10, 30, 245);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:12:37', 11, 31, 100);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:12:38', 11, 32, 100);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:12:39', 11, 33, 100);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:12:40', 11, 34, 155);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:12:41', 11, 35, 155);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:12:42', 11, 36, 155);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:12:43', 12, 31, 155);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:12:44', 12, 32, 155);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:12:45', 12, 33, 155);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:12:46', 12, 34, 100);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:12:47', 12, 35, 100);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:12:48', 12, 36, 100);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:13:01', 4, 37, 20);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:13:02', 8, 38, 20);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:13:03', 13, 39, 123);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:13:04', 14, 39, 111);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:13:05', 14, 40, 50);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:13:06', 15, 41, 50);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:13:07', 15, 41, 50);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:13:08', 15, 42, 50);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:13:09', 15, 42, 50);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:13:10', 16, 42, 50);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:13:11', 16, 42, 50);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:13:12', 16, 43, 50);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:13:13', 16, 43, 50);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:13:14', 16, 47, 50);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:13:15', 17, 44, 10);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:13:16', 17, 44, 10);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:13:17', 17, 45, 10);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:13:18', 17, 45, 10);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-02-01 01:13:19', 18, 45, 10);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-03-01 01:13:20', 18, 45, 10);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-04-01 01:13:21', 18, 46, 10);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-05-01 01:13:22', 18, 46, 10);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-06-11 01:13:23', 19, 44, 10);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:13:24', 19, 44, 10);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:13:25', 19, 45, 10);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:13:26', 19, 45, 10);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-02-01 01:13:27', 20, 45, 10);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-03-01 01:13:28', 20, 45, 10);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-04-01 01:13:29', 20, 46, 10);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-05-01 01:13:30', 20, 46, 10);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-02-01 01:13:31', 21, 49, 50);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-02-02 01:13:32', 21, 49, 50);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-02-03 01:13:33', 21, 50, 50);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-02-04 01:13:34', 21, 50, 50);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-02-05 01:13:35', 21, 48, 1);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2000-01-01 01:13:36', 22, 50, 50);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2001-01-01 01:13:37', 22, 50, 50);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2002-01-01 01:13:38', 22, 51, 50);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2002-06-01 01:13:39', 22, 51, 50);
INSERT INTO utb (b_datetime, b_q_id, b_v_id, b_vol) VALUES (timestamp'2003-01-01 01:13:05', 4, 37, 185);

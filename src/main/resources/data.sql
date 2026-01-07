DROP TABLE IF EXISTS TEST;

CREATE TABLE TEST (
                               id INT AUTO_INCREMENT  PRIMARY KEY,
                               username VARCHAR(250) NOT NULL,
                               password VARCHAR(250) NOT NULL

);

INSERT INTO TEST (USERNAME, PASSWORD) VALUES
                                           ('Lokesh', 'Gupta'),
                                           ('Deja', 'Vu'),
                                           ('Caption', 'America');
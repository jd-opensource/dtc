CREATE DATABASE if not exists dtc 
    DEFAULT CHARACTER SET utf8;

USE dtc;
CREATE TABLE if not exists dtc_opensource
(
    uid INT(11),
    name VARCHAR(50),
    city VARCHAR(50),
    sex INT(11),
    age INT(11)
);
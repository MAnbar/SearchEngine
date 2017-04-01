-- MySQL Script generated by MySQL Workbench
-- 04/01/17 18:37:24
-- Model: New Model    Version: 1.0
-- MySQL Workbench Forward Engineering

SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='TRADITIONAL,ALLOW_INVALID_DATES';

-- -----------------------------------------------------
-- Schema mydb
-- -----------------------------------------------------
-- -----------------------------------------------------
-- Schema searchengine
-- -----------------------------------------------------

-- -----------------------------------------------------
-- Schema searchengine
-- -----------------------------------------------------
CREATE SCHEMA IF NOT EXISTS `searchengine` DEFAULT CHARACTER SET utf8 ;
USE `searchengine` ;

-- -----------------------------------------------------
-- Table `searchengine`.`keywords`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `searchengine`.`keywords` (
  `Key_ID` INT(11) NOT NULL AUTO_INCREMENT,
  `KeyWord` VARCHAR(50) CHARACTER SET 'utf8' NOT NULL,
  PRIMARY KEY (`Key_ID`),
  UNIQUE INDEX `KeyWord_UNIQUE` (`KeyWord` ASC))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8
COLLATE = utf8_bin;


-- -----------------------------------------------------
-- Table `searchengine`.`urldata`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `searchengine`.`urldata` (
  `URL_ID` INT(11) NOT NULL AUTO_INCREMENT,
  `URL` VARCHAR(1000) CHARACTER SET 'utf8' NOT NULL,
  `Visited` CHAR(1) CHARACTER SET 'utf8' NOT NULL DEFAULT 'F',
  `Indexed` CHAR(1) CHARACTER SET 'utf8' NOT NULL DEFAULT 'F',
  `Downloaded` CHAR(1) CHARACTER SET 'utf8' NOT NULL DEFAULT 'F',
  `Priority` CHAR(1) CHARACTER SET 'utf8' NOT NULL DEFAULT 'L',
  PRIMARY KEY (`URL_ID`),
  UNIQUE INDEX `URL_UNIQUE` (`URL`(255) ASC))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8
COLLATE = utf8_bin;


-- -----------------------------------------------------
-- Table `searchengine`.`keydata`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `searchengine`.`keydata` (
  `KID` INT(11) NOT NULL,
  `UID` INT(11) NOT NULL,
  `Frequency` INT(11) NOT NULL,
  `KD_ID` INT(11) NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`KID`, `UID`),
  UNIQUE INDEX `KD_ID_UNIQUE` (`KD_ID` ASC),
  INDEX `URLID_idx` (`UID` ASC),
  CONSTRAINT `KeyID`
    FOREIGN KEY (`KID`)
    REFERENCES `searchengine`.`keywords` (`Key_ID`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `URLID`
    FOREIGN KEY (`UID`)
    REFERENCES `searchengine`.`urldata` (`URL_ID`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;


-- -----------------------------------------------------
-- Table `searchengine`.`ranking`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `searchengine`.`ranking` (
  `KDID` INT(11) NOT NULL,
  `Position` INT(11) NOT NULL,
  PRIMARY KEY (`KDID`, `Position`),
  CONSTRAINT `KDID`
    FOREIGN KEY (`KDID`)
    REFERENCES `searchengine`.`keydata` (`KD_ID`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;


-- -----------------------------------------------------
-- Table `searchengine`.`restricted`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `searchengine`.`restricted` (
  `RURL_id` INT(11) NOT NULL AUTO_INCREMENT,
  `rURL` VARCHAR(1000) NULL DEFAULT NULL,
  PRIMARY KEY (`RURL_id`),
  UNIQUE INDEX `rURL_UNIQUE` (`rURL`(255) ASC))
ENGINE = InnoDB
AUTO_INCREMENT = 1364
DEFAULT CHARACTER SET = utf8;


SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;

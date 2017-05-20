CREATE DATABASE `searchengine` /*!40100 DEFAULT CHARACTER SET utf8 */;
CREATE TABLE `keydata` (
  `KID` int(11) NOT NULL,
  `UID` int(11) NOT NULL,
  `Frequency` int(11) NOT NULL,
  `KD_ID` int(11) NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`KID`,`UID`),
  UNIQUE KEY `KD_ID_UNIQUE` (`KD_ID`),
  KEY `URLID_idx` (`UID`),
  CONSTRAINT `KeyID` FOREIGN KEY (`KID`) REFERENCES `keywords` (`Key_ID`) ON DELETE NO ACTION ON UPDATE NO ACTION,
  CONSTRAINT `URLID` FOREIGN KEY (`UID`) REFERENCES `urldata` (`URL_ID`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB AUTO_INCREMENT=576887 DEFAULT CHARSET=utf8;

CREATE TABLE `keywords` (
  `Key_ID` int(11) NOT NULL AUTO_INCREMENT,
  `KeyWord` varchar(50) CHARACTER SET utf8 NOT NULL,
  PRIMARY KEY (`Key_ID`),
  UNIQUE KEY `KeyWord_UNIQUE` (`KeyWord`)
) ENGINE=InnoDB AUTO_INCREMENT=69869 DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

CREATE TABLE `ranking` (
  `KDID` int(11) NOT NULL,
  `Position` int(11) NOT NULL,
  PRIMARY KEY (`KDID`,`Position`),
  CONSTRAINT `KDID` FOREIGN KEY (`KDID`) REFERENCES `keydata` (`KD_ID`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `restricted` (
  `RURL_id` int(11) NOT NULL AUTO_INCREMENT,
  `rURL` varchar(1000) DEFAULT NULL,
  PRIMARY KEY (`RURL_id`),
  UNIQUE KEY `rURL_UNIQUE` (`rURL`(255))
) ENGINE=InnoDB AUTO_INCREMENT=4760 DEFAULT CHARSET=utf8;

CREATE TABLE `urldata` (
  `URL_ID` int(11) NOT NULL AUTO_INCREMENT,
  `URL` varchar(2500) CHARACTER SET utf8 NOT NULL,
  `Visited` char(1) CHARACTER SET utf8 NOT NULL DEFAULT 'F',
  `Indexed` char(1) CHARACTER SET utf8 NOT NULL DEFAULT 'F',
  `Downloaded` char(1) CHARACTER SET utf8 NOT NULL DEFAULT 'F',
  `Priority` char(1) CHARACTER SET utf8 NOT NULL DEFAULT 'L',
  `Popularity` int(11) NOT NULL DEFAULT '1',
  PRIMARY KEY (`URL_ID`),
  UNIQUE KEY `URL_UNIQUE` (`URL`(255))
) ENGINE=InnoDB AUTO_INCREMENT=53039 DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
DELIMITER $$
CREATE DEFINER=`root`@`localhost` PROCEDURE `CreateTempTFIDFTable`(in Ranker varchar(20),in Word varchar(50),in IDF decimal)
BEGIN
				if Ranker='R1' then begin
                DROP TABLE IF EXISTS RTemp1;
                create table RTemp1  as select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' 
                from urldata, keydata where URL_ID=UID and URL_ID 
                in (select URL_ID from urldata, keydata, keywords where Key_ID=KID and URL_ID=UID and KeyWord=Word) group by URL) t1
          inner join      (select UID,Frequency from keydata, keywords
                where Key_ID=KID and KeyWord='Game') t2
                on t1.URL_ID = t2.UID) a;
                end; end if;
                
                if Ranker='R2' then begin
                DROP TABLE IF EXISTS RTemp2;
                create table RTemp2  as select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' from urldata, keydata where URL_ID=UID and URL_ID in (select UID from keydata, keywords where Key_ID=KID and KeyWord=Word)
                group by URL) t1
          inner join      (select UID,Frequency from keydata, keywords
                where Key_ID=KID and KeyWord='Game') t2
                on t1.URL_ID = t2.UID) a;
                end; end if;
                
                if Ranker='R3' then begin
                DROP TABLE IF EXISTS RTemp3;
                create table RTemp3  as select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' from urldata, keydata where URL_ID=UID and URL_ID in (select UID from keydata, keywords where Key_ID=KID and KeyWord=Word)
                group by URL) t1
          inner join      (select UID,Frequency from keydata, keywords
                where Key_ID=KID and KeyWord='Game') t2
                on t1.URL_ID = t2.UID) a;
                end; end if;
                
                if Ranker='R4' then begin
                DROP TABLE IF EXISTS RTemp4;
                create table RTemp4  as select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' from urldata, keydata where URL_ID=UID and URL_ID in (select UID from keydata, keywords where Key_ID=KID and KeyWord=Word)
                group by URL) t1
          inner join      (select UID,Frequency from keydata, keywords
                where Key_ID=KID and KeyWord='Game') t2
                on t1.URL_ID = t2.UID) a;
                end; end if;
                
                if Ranker='R5' then begin
                DROP TABLE IF EXISTS RTemp5;
                create table RTemp5  as select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' from urldata, keydata where URL_ID=UID and URL_ID in (select UID from keydata, keywords where Key_ID=KID and KeyWord=Word)
                group by URL) t1
          inner join      (select UID,Frequency from keydata, keywords
                where Key_ID=KID and KeyWord='Game') t2
                on t1.URL_ID = t2.UID) a;
                end; end if;
                
		        if Ranker='R6' then begin
                DROP TABLE IF EXISTS RTemp6;
                create table RTemp6  as select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' from urldata, keydata where URL_ID=UID and URL_ID in (select UID from keydata, keywords where Key_ID=KID and KeyWord=Word)
                group by URL) t1
          inner join      (select UID,Frequency from keydata, keywords
                where Key_ID=KID and KeyWord='Game') t2
                on t1.URL_ID = t2.UID) a;
                end; end if;
                
                if Ranker='R7' then begin
                DROP TABLE IF EXISTS RTemp7;
                create table RTemp7  as select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' from urldata, keydata where URL_ID=UID and URL_ID in (select UID from keydata, keywords where Key_ID=KID and KeyWord=Word)
                group by URL) t1
          inner join      (select UID,Frequency from keydata, keywords
                where Key_ID=KID and KeyWord='Game') t2
                on t1.URL_ID = t2.UID) a;
                end; end if;
                
                if Ranker='R8' then begin
                DROP TABLE IF EXISTS RTemp8;
                create table RTemp8  as select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' from urldata, keydata where URL_ID=UID and URL_ID in (select UID from keydata, keywords where Key_ID=KID and KeyWord=Word)
                group by URL) t1
          inner join      (select UID,Frequency from keydata, keywords
                where Key_ID=KID and KeyWord='Game') t2
                on t1.URL_ID = t2.UID) a;
                end; end if;
                
                if Ranker='R9' then begin
                DROP TABLE IF EXISTS RTemp9;
                create table RTemp9  as select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' from urldata, keydata where URL_ID=UID and URL_ID in (select UID from keydata, keywords where Key_ID=KID and KeyWord=Word)
                group by URL) t1
          inner join      (select UID,Frequency from keydata, keywords
                where Key_ID=KID and KeyWord='Game') t2
                on t1.URL_ID = t2.UID) a;
                end; end if;
                
                if Ranker='R10' then begin
                DROP TABLE IF EXISTS RTemp10;
                create table RTemp10  as select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' from urldata, keydata where URL_ID=UID and URL_ID in (select UID from keydata, keywords where Key_ID=KID and KeyWord=Word)
                group by URL) t1
          inner join      (select UID,Frequency from keydata, keywords
                where Key_ID=KID and KeyWord='Game') t2
                on t1.URL_ID = t2.UID) a;
                end; end if;
                
			    if Ranker='R11' then begin
                DROP TABLE IF EXISTS RTemp11;
                create table RTemp11  as select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' from urldata, keydata where URL_ID=UID and URL_ID in (select UID from keydata, keywords where Key_ID=KID and KeyWord=Word)
                group by URL) t1
          inner join      (select UID,Frequency from keydata, keywords
                where Key_ID=KID and KeyWord='Game') t2
                on t1.URL_ID = t2.UID) a;
                end; end if;
                
                if Ranker='R12' then begin
                DROP TABLE IF EXISTS RTemp12;
                create table RTemp12  as select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' from urldata, keydata where URL_ID=UID and URL_ID in (select UID from keydata, keywords where Key_ID=KID and KeyWord=Word)
                group by URL) t1
          inner join      (select UID,Frequency from keydata, keywords
                where Key_ID=KID and KeyWord='Game') t2
                on t1.URL_ID = t2.UID) a;
                end; end if;
                
                if Ranker='R13' then begin
                DROP TABLE IF EXISTS RTemp13;
                create table RTemp13  as select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' from urldata, keydata where URL_ID=UID and URL_ID in (select UID from keydata, keywords where Key_ID=KID and KeyWord=Word)
                group by URL) t1
          inner join      (select UID,Frequency from keydata, keywords
                where Key_ID=KID and KeyWord='Game') t2
                on t1.URL_ID = t2.UID) a;
                end; end if;
                
                if Ranker='R14' then begin
                DROP TABLE IF EXISTS RTemp14;
                create table RTemp14  as select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' from urldata, keydata where URL_ID=UID and URL_ID in (select UID from keydata, keywords where Key_ID=KID and KeyWord=Word)
                group by URL) t1
          inner join      (select UID,Frequency from keydata, keywords
                where Key_ID=KID and KeyWord='Game') t2
                on t1.URL_ID = t2.UID) a;
                end; end if;
                
                if Ranker='R15' then begin
                DROP TABLE IF EXISTS RTemp15;
                create table RTemp15  as select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' from urldata, keydata where URL_ID=UID and URL_ID in (select UID from keydata, keywords where Key_ID=KID and KeyWord=Word)
                group by URL) t1
          inner join      (select UID,Frequency from keydata, keywords
                where Key_ID=KID and KeyWord='Game') t2
                on t1.URL_ID = t2.UID) a;
                end; end if;
                
		        if Ranker='R16' then begin
                DROP TABLE IF EXISTS RTemp16;
                create table RTemp16  as select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' from urldata, keydata where URL_ID=UID and URL_ID in (select UID from keydata, keywords where Key_ID=KID and KeyWord=Word)
                group by URL) t1
          inner join      (select UID,Frequency from keydata, keywords
                where Key_ID=KID and KeyWord='Game') t2
                on t1.URL_ID = t2.UID) a;
                end; end if;
                
                if Ranker='R17' then begin
                DROP TABLE IF EXISTS RTemp17;
                create table RTemp17  as select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' from urldata, keydata where URL_ID=UID and URL_ID in (select UID from keydata, keywords where Key_ID=KID and KeyWord=Word)
                group by URL) t1
          inner join      (select UID,Frequency from keydata, keywords
                where Key_ID=KID and KeyWord='Game') t2
                on t1.URL_ID = t2.UID) a;
                end; end if;
                
                if Ranker='R18' then begin
                DROP TABLE IF EXISTS RTemp18;
                create table RTemp18  as select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' from urldata, keydata where URL_ID=UID and URL_ID in (select UID from keydata, keywords where Key_ID=KID and KeyWord=Word)
                group by URL) t1
          inner join      (select UID,Frequency from keydata, keywords
                where Key_ID=KID and KeyWord='Game') t2
                on t1.URL_ID = t2.UID) a;
                end; end if;
                
                if Ranker='R19' then begin
                DROP TABLE IF EXISTS RTemp19;
                create table RTemp19  as select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' from urldata, keydata where URL_ID=UID and URL_ID in (select UID from keydata, keywords where Key_ID=KID and KeyWord=Word)
                group by URL) t1
          inner join      (select UID,Frequency from keydata, keywords
                where Key_ID=KID and KeyWord='Game') t2
                on t1.URL_ID = t2.UID) a;
                end; end if;
                
                if Ranker='R20' then begin
                DROP TABLE IF EXISTS RTemp20;
                create table RTemp20  as select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' from urldata, keydata where URL_ID=UID and URL_ID in (select UID from keydata, keywords where Key_ID=KID and KeyWord=Word)
                group by URL) t1
          inner join      (select UID,Frequency from keydata, keywords
                where Key_ID=KID and KeyWord='Game') t2
                on t1.URL_ID = t2.UID) a;
                end; end if;
END$$
DELIMITER ;

DELIMITER $$
CREATE DEFINER=`root`@`localhost` PROCEDURE `CreateTempTFIDFTableLegacy`(in Ranker varchar(20),in Word varchar(50),in IDF decimal)
BEGIN
				if Ranker='R1' then begin
                DROP TABLE IF EXISTS RTemp1;
                create table RTemp1  as select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' 
                from urldata, keydata where URL_ID=UID and URL_ID 
                in (select URL_ID from urldata, keydata, keywords where Key_ID=KID and URL_ID=UID and KeyWord=Word) group by URL) t1
                inner join
                (select URL_ID,Frequency from urldata, keydata, keywords
                where Key_ID=KID and URL_ID=UID and KeyWord=Word) t2
                on t1.URL_ID = t2.URL_ID) a;
                end; end if;
                
                if Ranker='R2' then begin
                DROP TABLE IF EXISTS RTemp2;
                create table RTemp2  as select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' from urldata, keydata where URL_ID=UID and URL_ID in (select URL_ID from urldata, keydata, keywords 
                where Key_ID=KID and URL_ID=UID and KeyWord=Word)
                group by URL) t1
                inner join
                (select URL_ID,Frequency from urldata, keydata, keywords
                where Key_ID=KID and URL_ID=UID and KeyWord=Word) t2
                on t1.URL_ID = t2.URL_ID) a;
                end; end if;
                
                if Ranker='R3' then begin
                DROP TABLE IF EXISTS RTemp3;
                create table RTemp3  as select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' from urldata, keydata where URL_ID=UID and URL_ID in (select URL_ID from urldata, keydata, keywords 
                where Key_ID=KID and URL_ID=UID and KeyWord=Word)
                group by URL) t1
                inner join
                (select URL_ID,Frequency from urldata, keydata, keywords
                where Key_ID=KID and URL_ID=UID and KeyWord=Word) t2
                on t1.URL_ID = t2.URL_ID) a;
                end; end if;
                
                if Ranker='R4' then begin
                DROP TABLE IF EXISTS RTemp4;
                create table RTemp4  as select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' from urldata, keydata where URL_ID=UID and URL_ID in (select URL_ID from urldata, keydata, keywords 
                where Key_ID=KID and URL_ID=UID and KeyWord=Word)
                group by URL) t1
                inner join
                (select URL_ID,Frequency from urldata, keydata, keywords
                where Key_ID=KID and URL_ID=UID and KeyWord=Word) t2
                on t1.URL_ID = t2.URL_ID) a;
                end; end if;
                
                if Ranker='R5' then begin
                DROP TABLE IF EXISTS RTemp5;
                create table RTemp5  as select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' from urldata, keydata where URL_ID=UID and URL_ID in (select URL_ID from urldata, keydata, keywords 
                where Key_ID=KID and URL_ID=UID and KeyWord=Word)
                group by URL) t1
                inner join
                (select URL_ID,Frequency from urldata, keydata, keywords
                where Key_ID=KID and URL_ID=UID and KeyWord=Word) t2
                on t1.URL_ID = t2.URL_ID) a;
                end; end if;
                
		        if Ranker='R6' then begin
                DROP TABLE IF EXISTS RTemp6;
                create table RTemp6  as select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' from urldata, keydata where URL_ID=UID and URL_ID in (select URL_ID from urldata, keydata, keywords 
                where Key_ID=KID and URL_ID=UID and KeyWord=Word)
                group by URL) t1
                inner join
                (select URL_ID,Frequency from urldata, keydata, keywords
                where Key_ID=KID and URL_ID=UID and KeyWord=Word) t2
                on t1.URL_ID = t2.URL_ID) a;
                end; end if;
                
                if Ranker='R7' then begin
                DROP TABLE IF EXISTS RTemp7;
                create table RTemp7  as select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' from urldata, keydata where URL_ID=UID and URL_ID in (select URL_ID from urldata, keydata, keywords 
                where Key_ID=KID and URL_ID=UID and KeyWord=Word)
                group by URL) t1
                inner join
                (select URL_ID,Frequency from urldata, keydata, keywords
                where Key_ID=KID and URL_ID=UID and KeyWord=Word) t2
                on t1.URL_ID = t2.URL_ID) a;
                end; end if;
                
                if Ranker='R8' then begin
                DROP TABLE IF EXISTS RTemp8;
                create table RTemp8  as select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' from urldata, keydata where URL_ID=UID and URL_ID in (select URL_ID from urldata, keydata, keywords 
                where Key_ID=KID and URL_ID=UID and KeyWord=Word)
                group by URL) t1
                inner join
                (select URL_ID,Frequency from urldata, keydata, keywords
                where Key_ID=KID and URL_ID=UID and KeyWord=Word) t2
                on t1.URL_ID = t2.URL_ID) a;
                end; end if;
                
                if Ranker='R9' then begin
                DROP TABLE IF EXISTS RTemp9;
                create table RTemp9  as select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' from urldata, keydata where URL_ID=UID and URL_ID in (select URL_ID from urldata, keydata, keywords 
                where Key_ID=KID and URL_ID=UID and KeyWord=Word)
                group by URL) t1
                inner join
                (select URL_ID,Frequency from urldata, keydata, keywords
                where Key_ID=KID and URL_ID=UID and KeyWord=Word) t2
                on t1.URL_ID = t2.URL_ID) a;
                end; end if;
                
                if Ranker='R10' then begin
                DROP TABLE IF EXISTS RTemp10;
                create table RTemp10  as select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' from urldata, keydata where URL_ID=UID and URL_ID in (select URL_ID from urldata, keydata, keywords 
                where Key_ID=KID and URL_ID=UID and KeyWord=Word)
                group by URL) t1
                inner join
                (select URL_ID,Frequency from urldata, keydata, keywords
                where Key_ID=KID and URL_ID=UID and KeyWord=Word) t2
                on t1.URL_ID = t2.URL_ID) a;
                end; end if;
                
			    if Ranker='R11' then begin
                DROP TABLE IF EXISTS RTemp11;
                create table RTemp11  as select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' from urldata, keydata where URL_ID=UID and URL_ID in (select URL_ID from urldata, keydata, keywords 
                where Key_ID=KID and URL_ID=UID and KeyWord=Word)
                group by URL) t1
                inner join
                (select URL_ID,Frequency from urldata, keydata, keywords
                where Key_ID=KID and URL_ID=UID and KeyWord=Word) t2
                on t1.URL_ID = t2.URL_ID) a;
                end; end if;
                
                if Ranker='R12' then begin
                DROP TABLE IF EXISTS RTemp12;
                create table RTemp12  as select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' from urldata, keydata where URL_ID=UID and URL_ID in (select URL_ID from urldata, keydata, keywords 
                where Key_ID=KID and URL_ID=UID and KeyWord=Word)
                group by URL) t1
                inner join
                (select URL_ID,Frequency from urldata, keydata, keywords
                where Key_ID=KID and URL_ID=UID and KeyWord=Word) t2
                on t1.URL_ID = t2.URL_ID) a;
                end; end if;
                
                if Ranker='R13' then begin
                DROP TABLE IF EXISTS RTemp13;
                create table RTemp13  as select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' from urldata, keydata where URL_ID=UID and URL_ID in (select URL_ID from urldata, keydata, keywords 
                where Key_ID=KID and URL_ID=UID and KeyWord=Word)
                group by URL) t1
                inner join
                (select URL_ID,Frequency from urldata, keydata, keywords
                where Key_ID=KID and URL_ID=UID and KeyWord=Word) t2
                on t1.URL_ID = t2.URL_ID) a;
                end; end if;
                
                if Ranker='R14' then begin
                DROP TABLE IF EXISTS RTemp14;
                create table RTemp14  as select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' from urldata, keydata where URL_ID=UID and URL_ID in (select URL_ID from urldata, keydata, keywords 
                where Key_ID=KID and URL_ID=UID and KeyWord=Word)
                group by URL) t1
                inner join
                (select URL_ID,Frequency from urldata, keydata, keywords
                where Key_ID=KID and URL_ID=UID and KeyWord=Word) t2
                on t1.URL_ID = t2.URL_ID) a;
                end; end if;
                
                if Ranker='R15' then begin
                DROP TABLE IF EXISTS RTemp15;
                create table RTemp15  as select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' from urldata, keydata where URL_ID=UID and URL_ID in (select URL_ID from urldata, keydata, keywords 
                where Key_ID=KID and URL_ID=UID and KeyWord=Word)
                group by URL) t1
                inner join
                (select URL_ID,Frequency from urldata, keydata, keywords
                where Key_ID=KID and URL_ID=UID and KeyWord=Word) t2
                on t1.URL_ID = t2.URL_ID) a;
                end; end if;
                
		        if Ranker='R16' then begin
                DROP TABLE IF EXISTS RTemp16;
                create table RTemp16  as select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' from urldata, keydata where URL_ID=UID and URL_ID in (select URL_ID from urldata, keydata, keywords 
                where Key_ID=KID and URL_ID=UID and KeyWord=Word)
                group by URL) t1
                inner join
                (select URL_ID,Frequency from urldata, keydata, keywords
                where Key_ID=KID and URL_ID=UID and KeyWord=Word) t2
                on t1.URL_ID = t2.URL_ID) a;
                end; end if;
                
                if Ranker='R17' then begin
                DROP TABLE IF EXISTS RTemp17;
                create table RTemp17  as select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' from urldata, keydata where URL_ID=UID and URL_ID in (select URL_ID from urldata, keydata, keywords 
                where Key_ID=KID and URL_ID=UID and KeyWord=Word)
                group by URL) t1
                inner join
                (select URL_ID,Frequency from urldata, keydata, keywords
                where Key_ID=KID and URL_ID=UID and KeyWord=Word) t2
                on t1.URL_ID = t2.URL_ID) a;
                end; end if;
                
                if Ranker='R18' then begin
                DROP TABLE IF EXISTS RTemp18;
                create table RTemp18  as select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' from urldata, keydata where URL_ID=UID and URL_ID in (select URL_ID from urldata, keydata, keywords 
                where Key_ID=KID and URL_ID=UID and KeyWord=Word)
                group by URL) t1
                inner join
                (select URL_ID,Frequency from urldata, keydata, keywords
                where Key_ID=KID and URL_ID=UID and KeyWord=Word) t2
                on t1.URL_ID = t2.URL_ID) a;
                end; end if;
                
                if Ranker='R19' then begin
                DROP TABLE IF EXISTS RTemp19;
                create table RTemp19  as select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' from urldata, keydata where URL_ID=UID and URL_ID in (select URL_ID from urldata, keydata, keywords 
                where Key_ID=KID and URL_ID=UID and KeyWord=Word)
                group by URL) t1
                inner join
                (select URL_ID,Frequency from urldata, keydata, keywords
                where Key_ID=KID and URL_ID=UID and KeyWord=Word) t2
                on t1.URL_ID = t2.URL_ID) a;
                end; end if;
                
                if Ranker='R20' then begin
                DROP TABLE IF EXISTS RTemp20;
                create table RTemp20  as select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' from urldata, keydata where URL_ID=UID and URL_ID in (select URL_ID from urldata, keydata, keywords 
                where Key_ID=KID and URL_ID=UID and KeyWord=Word)
                group by URL) t1
                inner join
                (select URL_ID,Frequency from urldata, keydata, keywords
                where Key_ID=KID and URL_ID=UID and KeyWord=Word) t2
                on t1.URL_ID = t2.URL_ID) a;
                end; end if;
END$$
DELIMITER ;

DELIMITER $$
CREATE DEFINER=`root`@`localhost` PROCEDURE `InsertIntoTempTFIDFTable`(in Ranker varchar(20),in Word varchar(50),in IDF decimal)
BEGIN
if Ranker ='R1' then begin
Insert Into RTemp1 (select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' 
                from urldata, keydata where URL_ID=UID and URL_ID 
               in (select UID from keydata, keywords where Key_ID=KID and KeyWord=Word) group by URL) t1
                inner join
                (select UID,Frequency from keydata, keywords
                where Key_ID=KID and KeyWord=Word) t2
                on t1.URL_ID = t2.UID) a);
                end; end if;
                
                if Ranker ='R2' then begin
Insert Into RTemp2 (select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' 
                from urldata, keydata where URL_ID=UID and URL_ID 
               in (select UID from keydata, keywords where Key_ID=KID and KeyWord=Word) group by URL) t1
                inner join
                (select UID,Frequency from keydata, keywords
                where Key_ID=KID and KeyWord=Word) t2
                on t1.URL_ID = t2.UID) a);
                end; end if;
                
                if Ranker ='R3' then begin
Insert Into RTemp3 (select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' 
                from urldata, keydata where URL_ID=UID and URL_ID 
               in (select UID from keydata, keywords where Key_ID=KID and KeyWord=Word) group by URL) t1
                inner join
                (select UID,Frequency from keydata, keywords
                where Key_ID=KID and KeyWord=Word) t2
                on t1.URL_ID = t2.UID) a);
                end; end if;
                
                if Ranker ='R4' then begin
Insert Into RTemp4 (select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' 
                from urldata, keydata where URL_ID=UID and URL_ID 
               in (select UID from keydata, keywords where Key_ID=KID and KeyWord=Word) group by URL) t1
                inner join
                (select UID,Frequency from keydata, keywords
                where Key_ID=KID and KeyWord=Word) t2
                on t1.URL_ID = t2.UID) a);
                end; end if;
                
                if Ranker ='R5' then begin
Insert Into RTemp5 (select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' 
                from urldata, keydata where URL_ID=UID and URL_ID 
               in (select UID from keydata, keywords where Key_ID=KID and KeyWord=Word) group by URL) t1
                inner join
                (select UID,Frequency from keydata, keywords
                where Key_ID=KID and KeyWord=Word) t2
                on t1.URL_ID = t2.UID) a);
                end; end if;
                
                if Ranker ='R6' then begin
Insert Into RTemp6 (select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' 
                from urldata, keydata where URL_ID=UID and URL_ID 
               in (select UID from keydata, keywords where Key_ID=KID and KeyWord=Word) group by URL) t1
                inner join
                (select UID,Frequency from keydata, keywords
                where Key_ID=KID and KeyWord=Word) t2
                on t1.URL_ID = t2.UID) a);
                end; end if;
                
                if Ranker ='R7' then begin
Insert Into RTemp7 (select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' 
                from urldata, keydata where URL_ID=UID and URL_ID 
               in (select UID from keydata, keywords where Key_ID=KID and KeyWord=Word) group by URL) t1
                inner join
                (select UID,Frequency from keydata, keywords
                where Key_ID=KID and KeyWord=Word) t2
                on t1.URL_ID = t2.UID) a);
                end; end if;
                
                if Ranker ='R8' then begin
Insert Into RTemp8 (select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' 
                from urldata, keydata where URL_ID=UID and URL_ID 
               in (select UID from keydata, keywords where Key_ID=KID and KeyWord=Word) group by URL) t1
                inner join
                (select UID,Frequency from keydata, keywords
                where Key_ID=KID and KeyWord=Word) t2
                on t1.URL_ID = t2.UID) a);
                end; end if;
                
                if Ranker ='R9' then begin
Insert Into RTemp9 (select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' 
                from urldata, keydata where URL_ID=UID and URL_ID 
               in (select UID from keydata, keywords where Key_ID=KID and KeyWord=Word) group by URL) t1
                inner join
                (select UID,Frequency from keydata, keywords
                where Key_ID=KID and KeyWord=Word) t2
                on t1.URL_ID = t2.UID) a);
                end; end if;
                
                if Ranker ='R10' then begin
Insert Into RTemp10 (select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' 
                from urldata, keydata where URL_ID=UID and URL_ID 
               in (select UID from keydata, keywords where Key_ID=KID and KeyWord=Word) group by URL) t1
                inner join
                (select UID,Frequency from keydata, keywords
                where Key_ID=KID and KeyWord=Word) t2
                on t1.URL_ID = t2.UID) a);
                end; end if;
                
                if Ranker ='R11' then begin
Insert Into RTemp11 (select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' 
                from urldata, keydata where URL_ID=UID and URL_ID 
               in (select UID from keydata, keywords where Key_ID=KID and KeyWord=Word) group by URL) t1
                inner join
                (select UID,Frequency from keydata, keywords
                where Key_ID=KID and KeyWord=Word) t2
                on t1.URL_ID = t2.UID) a);
                end; end if;
                
                if Ranker ='R12' then begin
Insert Into RTemp12 (select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' 
                from urldata, keydata where URL_ID=UID and URL_ID 
               in (select UID from keydata, keywords where Key_ID=KID and KeyWord=Word) group by URL) t1
                inner join
                (select UID,Frequency from keydata, keywords
                where Key_ID=KID and KeyWord=Word) t2
                on t1.URL_ID = t2.UID) a);
                end; end if;
                
                if Ranker ='R13' then begin
Insert Into RTemp13 (select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' 
                from urldata, keydata where URL_ID=UID and URL_ID 
               in (select UID from keydata, keywords where Key_ID=KID and KeyWord=Word) group by URL) t1
                inner join
                (select UID,Frequency from keydata, keywords
                where Key_ID=KID and KeyWord=Word) t2
                on t1.URL_ID = t2.UID) a);
                end; end if;
                
                if Ranker ='R14' then begin
Insert Into RTemp14 (select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' 
                from urldata, keydata where URL_ID=UID and URL_ID 
               in (select UID from keydata, keywords where Key_ID=KID and KeyWord=Word) group by URL) t1
                inner join
                (select UID,Frequency from keydata, keywords
                where Key_ID=KID and KeyWord=Word) t2
                on t1.URL_ID = t2.UID) a);
                end; end if;
                
                if Ranker ='R15' then begin
Insert Into RTemp15 (select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' 
                from urldata, keydata where URL_ID=UID and URL_ID 
               in (select UID from keydata, keywords where Key_ID=KID and KeyWord=Word) group by URL) t1
                inner join
                (select UID,Frequency from keydata, keywords
                where Key_ID=KID and KeyWord=Word) t2
                on t1.URL_ID = t2.UID) a);
                end; end if;
                
                if Ranker ='R16' then begin
Insert Into RTemp16 (select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' 
                from urldata, keydata where URL_ID=UID and URL_ID 
               in (select UID from keydata, keywords where Key_ID=KID and KeyWord=Word) group by URL) t1
                inner join
                (select UID,Frequency from keydata, keywords
                where Key_ID=KID and KeyWord=Word) t2
                on t1.URL_ID = t2.UID) a);
                end; end if;
                
                if Ranker ='R17' then begin
Insert Into RTemp17 (select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' 
                from urldata, keydata where URL_ID=UID and URL_ID 
               in (select UID from keydata, keywords where Key_ID=KID and KeyWord=Word) group by URL) t1
                inner join
                (select UID,Frequency from keydata, keywords
                where Key_ID=KID and KeyWord=Word) t2
                on t1.URL_ID = t2.UID) a);
                end; end if;
                
                if Ranker ='R18' then begin
Insert Into RTemp18 (select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' 
                from urldata, keydata where URL_ID=UID and URL_ID 
               in (select UID from keydata, keywords where Key_ID=KID and KeyWord=Word) group by URL) t1
                inner join
                (select UID,Frequency from keydata, keywords
                where Key_ID=KID and KeyWord=Word) t2
                on t1.URL_ID = t2.UID) a);
                end; end if;
                
                if Ranker ='R19' then begin
Insert Into RTemp19 (select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' 
                from urldata, keydata where URL_ID=UID and URL_ID 
               in (select UID from keydata, keywords where Key_ID=KID and KeyWord=Word) group by URL) t1
                inner join
                (select UID,Frequency from keydata, keywords
                where Key_ID=KID and KeyWord=Word) t2
                on t1.URL_ID = t2.UID) a);
                end; end if;
                
                if Ranker ='R20' then begin
Insert Into RTemp20 (select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' 
                from urldata, keydata where URL_ID=UID and URL_ID 
               in (select UID from keydata, keywords where Key_ID=KID and KeyWord=Word) group by URL) t1
                inner join
                (select UID,Frequency from keydata, keywords
                where Key_ID=KID and KeyWord=Word) t2
                on t1.URL_ID = t2.UID) a);
                end; end if;
END$$
DELIMITER ;

DELIMITER $$
CREATE DEFINER=`root`@`localhost` PROCEDURE `InsertIntoTempTFIDFTableLegacy`(in Ranker varchar(20),in Word varchar(50),in IDF decimal)
BEGIN
if Ranker ='R1' then begin
Insert Into RTemp1 (select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' 
                from urldata, keydata where URL_ID=UID and URL_ID 
                in (select URL_ID from urldata, keydata, keywords where Key_ID=KID and URL_ID=UID and KeyWord=Word) group by URL) t1
                inner join
                (select URL_ID,Frequency from urldata, keydata, keywords
                where Key_ID=KID and URL_ID=UID and KeyWord=Word) t2
                on t1.URL_ID = t2.URL_ID) a);
                end; end if;
                
                if Ranker ='R2' then begin
Insert Into RTemp2 (select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' 
                from urldata, keydata where URL_ID=UID and URL_ID 
                in (select URL_ID from urldata, keydata, keywords where Key_ID=KID and URL_ID=UID and KeyWord=Word) group by URL) t1
                inner join
                (select URL_ID,Frequency from urldata, keydata, keywords
                where Key_ID=KID and URL_ID=UID and KeyWord=Word) t2
                on t1.URL_ID = t2.URL_ID) a);
                end; end if;
                
                if Ranker ='R3' then begin
Insert Into RTemp3 (select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' 
                from urldata, keydata where URL_ID=UID and URL_ID 
                in (select URL_ID from urldata, keydata, keywords where Key_ID=KID and URL_ID=UID and KeyWord=Word) group by URL) t1
                inner join
                (select URL_ID,Frequency from urldata, keydata, keywords
                where Key_ID=KID and URL_ID=UID and KeyWord=Word) t2
                on t1.URL_ID = t2.URL_ID) a);
                end; end if;
                
                if Ranker ='R4' then begin
Insert Into RTemp4 (select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' 
                from urldata, keydata where URL_ID=UID and URL_ID 
                in (select URL_ID from urldata, keydata, keywords where Key_ID=KID and URL_ID=UID and KeyWord=Word) group by URL) t1
                inner join
                (select URL_ID,Frequency from urldata, keydata, keywords
                where Key_ID=KID and URL_ID=UID and KeyWord=Word) t2
                on t1.URL_ID = t2.URL_ID) a);
                end; end if;
                
                if Ranker ='R5' then begin
Insert Into RTemp5 (select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' 
                from urldata, keydata where URL_ID=UID and URL_ID 
                in (select URL_ID from urldata, keydata, keywords where Key_ID=KID and URL_ID=UID and KeyWord=Word) group by URL) t1
                inner join
                (select URL_ID,Frequency from urldata, keydata, keywords
                where Key_ID=KID and URL_ID=UID and KeyWord=Word) t2
                on t1.URL_ID = t2.URL_ID) a);
                end; end if;
                
                if Ranker ='R6' then begin
Insert Into RTemp6 (select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' 
                from urldata, keydata where URL_ID=UID and URL_ID 
                in (select URL_ID from urldata, keydata, keywords where Key_ID=KID and URL_ID=UID and KeyWord=Word) group by URL) t1
                inner join
                (select URL_ID,Frequency from urldata, keydata, keywords
                where Key_ID=KID and URL_ID=UID and KeyWord=Word) t2
                on t1.URL_ID = t2.URL_ID) a);
                end; end if;
                
                if Ranker ='R7' then begin
Insert Into RTemp7 (select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' 
                from urldata, keydata where URL_ID=UID and URL_ID 
                in (select URL_ID from urldata, keydata, keywords where Key_ID=KID and URL_ID=UID and KeyWord=Word) group by URL) t1
                inner join
                (select URL_ID,Frequency from urldata, keydata, keywords
                where Key_ID=KID and URL_ID=UID and KeyWord=Word) t2
                on t1.URL_ID = t2.URL_ID) a);
                end; end if;
                
                if Ranker ='R8' then begin
Insert Into RTemp8 (select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' 
                from urldata, keydata where URL_ID=UID and URL_ID 
                in (select URL_ID from urldata, keydata, keywords where Key_ID=KID and URL_ID=UID and KeyWord=Word) group by URL) t1
                inner join
                (select URL_ID,Frequency from urldata, keydata, keywords
                where Key_ID=KID and URL_ID=UID and KeyWord=Word) t2
                on t1.URL_ID = t2.URL_ID) a);
                end; end if;
                
                if Ranker ='R9' then begin
Insert Into RTemp9 (select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' 
                from urldata, keydata where URL_ID=UID and URL_ID 
                in (select URL_ID from urldata, keydata, keywords where Key_ID=KID and URL_ID=UID and KeyWord=Word) group by URL) t1
                inner join
                (select URL_ID,Frequency from urldata, keydata, keywords
                where Key_ID=KID and URL_ID=UID and KeyWord=Word) t2
                on t1.URL_ID = t2.URL_ID) a);
                end; end if;
                
                if Ranker ='R10' then begin
Insert Into RTemp10 (select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' 
                from urldata, keydata where URL_ID=UID and URL_ID 
                in (select URL_ID from urldata, keydata, keywords where Key_ID=KID and URL_ID=UID and KeyWord=Word) group by URL) t1
                inner join
                (select URL_ID,Frequency from urldata, keydata, keywords
                where Key_ID=KID and URL_ID=UID and KeyWord=Word) t2
                on t1.URL_ID = t2.URL_ID) a);
                end; end if;
                
                if Ranker ='R11' then begin
Insert Into RTemp11 (select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' 
                from urldata, keydata where URL_ID=UID and URL_ID 
                in (select URL_ID from urldata, keydata, keywords where Key_ID=KID and URL_ID=UID and KeyWord=Word) group by URL) t1
                inner join
                (select URL_ID,Frequency from urldata, keydata, keywords
                where Key_ID=KID and URL_ID=UID and KeyWord=Word) t2
                on t1.URL_ID = t2.URL_ID) a);
                end; end if;
                
                if Ranker ='R12' then begin
Insert Into RTemp12 (select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' 
                from urldata, keydata where URL_ID=UID and URL_ID 
                in (select URL_ID from urldata, keydata, keywords where Key_ID=KID and URL_ID=UID and KeyWord=Word) group by URL) t1
                inner join
                (select URL_ID,Frequency from urldata, keydata, keywords
                where Key_ID=KID and URL_ID=UID and KeyWord=Word) t2
                on t1.URL_ID = t2.URL_ID) a);
                end; end if;
                
                if Ranker ='R13' then begin
Insert Into RTemp13 (select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' 
                from urldata, keydata where URL_ID=UID and URL_ID 
                in (select URL_ID from urldata, keydata, keywords where Key_ID=KID and URL_ID=UID and KeyWord=Word) group by URL) t1
                inner join
                (select URL_ID,Frequency from urldata, keydata, keywords
                where Key_ID=KID and URL_ID=UID and KeyWord=Word) t2
                on t1.URL_ID = t2.URL_ID) a);
                end; end if;
                
                if Ranker ='R14' then begin
Insert Into RTemp14 (select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' 
                from urldata, keydata where URL_ID=UID and URL_ID 
                in (select URL_ID from urldata, keydata, keywords where Key_ID=KID and URL_ID=UID and KeyWord=Word) group by URL) t1
                inner join
                (select URL_ID,Frequency from urldata, keydata, keywords
                where Key_ID=KID and URL_ID=UID and KeyWord=Word) t2
                on t1.URL_ID = t2.URL_ID) a);
                end; end if;
                
                if Ranker ='R15' then begin
Insert Into RTemp15 (select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' 
                from urldata, keydata where URL_ID=UID and URL_ID 
                in (select URL_ID from urldata, keydata, keywords where Key_ID=KID and URL_ID=UID and KeyWord=Word) group by URL) t1
                inner join
                (select URL_ID,Frequency from urldata, keydata, keywords
                where Key_ID=KID and URL_ID=UID and KeyWord=Word) t2
                on t1.URL_ID = t2.URL_ID) a);
                end; end if;
                
                if Ranker ='R16' then begin
Insert Into RTemp16 (select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' 
                from urldata, keydata where URL_ID=UID and URL_ID 
                in (select URL_ID from urldata, keydata, keywords where Key_ID=KID and URL_ID=UID and KeyWord=Word) group by URL) t1
                inner join
                (select URL_ID,Frequency from urldata, keydata, keywords
                where Key_ID=KID and URL_ID=UID and KeyWord=Word) t2
                on t1.URL_ID = t2.URL_ID) a);
                end; end if;
                
                if Ranker ='R17' then begin
Insert Into RTemp17 (select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' 
                from urldata, keydata where URL_ID=UID and URL_ID 
                in (select URL_ID from urldata, keydata, keywords where Key_ID=KID and URL_ID=UID and KeyWord=Word) group by URL) t1
                inner join
                (select URL_ID,Frequency from urldata, keydata, keywords
                where Key_ID=KID and URL_ID=UID and KeyWord=Word) t2
                on t1.URL_ID = t2.URL_ID) a);
                end; end if;
                
                if Ranker ='R18' then begin
Insert Into RTemp18 (select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' 
                from urldata, keydata where URL_ID=UID and URL_ID 
                in (select URL_ID from urldata, keydata, keywords where Key_ID=KID and URL_ID=UID and KeyWord=Word) group by URL) t1
                inner join
                (select URL_ID,Frequency from urldata, keydata, keywords
                where Key_ID=KID and URL_ID=UID and KeyWord=Word) t2
                on t1.URL_ID = t2.URL_ID) a);
                end; end if;
                
                if Ranker ='R19' then begin
Insert Into RTemp19 (select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' 
                from urldata, keydata where URL_ID=UID and URL_ID 
                in (select URL_ID from urldata, keydata, keywords where Key_ID=KID and URL_ID=UID and KeyWord=Word) group by URL) t1
                inner join
                (select URL_ID,Frequency from urldata, keydata, keywords
                where Key_ID=KID and URL_ID=UID and KeyWord=Word) t2
                on t1.URL_ID = t2.URL_ID) a);
                end; end if;
                
                if Ranker ='R20' then begin
Insert Into RTemp20 (select a.URL_ID,a.URL,(CAST(a.Frequency AS decimal(10,6)) / a.URLTerms * IDF) as 'TF' from (select t1.URL_ID,t1.URL,t2.Frequency,t1.URLTerms
                from (select URL,URL_ID,sum(Frequency)as 'URLTerms' 
                from urldata, keydata where URL_ID=UID and URL_ID 
                in (select URL_ID from urldata, keydata, keywords where Key_ID=KID and URL_ID=UID and KeyWord=Word) group by URL) t1
                inner join
                (select URL_ID,Frequency from urldata, keydata, keywords
                where Key_ID=KID and URL_ID=UID and KeyWord=Word) t2
                on t1.URL_ID = t2.URL_ID) a);
                end; end if;
END$$
DELIMITER ;

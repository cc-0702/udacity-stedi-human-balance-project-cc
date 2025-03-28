{\rtf1\ansi\ansicpg1252\cocoartf2761
\cocoatextscaling0\cocoaplatform0{\fonttbl\f0\fnil\fcharset0 Monaco;}
{\colortbl;\red255\green255\blue255;\red13\green16\blue20;\red255\green255\blue255;}
{\*\expandedcolortbl;;\cssrgb\c5882\c7843\c10196;\cssrgb\c100000\c100000\c100000;}
\paperw11900\paperh16840\margl1440\margr1440\vieww11520\viewh8400\viewkind0
\deftab720
\pard\pardeftab720\partightenfactor0

\f0\fs24 \cf2 \cb3 \expnd0\expndtw0\kerning0
\outl0\strokewidth0 \strokec2 CREATE EXTERNAL TABLE `customer_landing`(\
  `customername` string COMMENT 'from deserializer', \
  `email` string COMMENT 'from deserializer', \
  `phone` string COMMENT 'from deserializer', \
  `birthday` string COMMENT 'from deserializer', \
  `serialnumber` string COMMENT 'from deserializer', \
  `registrationdate` bigint COMMENT 'from deserializer', \
  `lastupdatedate` bigint COMMENT 'from deserializer', \
  `sharewithresearchasofdate` bigint COMMENT 'from deserializer', \
  `sharewithpublicasofdate` bigint COMMENT 'from deserializer', \
  `sharewithfriendsasofdate` bigint COMMENT 'from deserializer')\
ROW FORMAT SERDE \
  'org.openx.data.jsonserde.JsonSerDe' \
STORED AS INPUTFORMAT \
  'org.apache.hadoop.mapred.TextInputFormat' \
OUTPUTFORMAT \
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'\
LOCATION\
  's3://stedi-project-cc-data/customer_landing/'\
TBLPROPERTIES (\
  'classification'='json')}
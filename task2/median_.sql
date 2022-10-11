CREATE temp TABLE IF NOT EXISTS  facebook_temp (
	spend decimal,
	rowNB serial primary key
);

INSERT INTO facebook_temp (
    SELECT spend FROM facebook ORDER BY spend
    WHERE EXTRACT(MONTH FROM facebook.date) = 08);

SELECT sum(facebook_rowNB.spend) / count(facebook_rowNB.rowNB ) as d

FROM (select facebook_temp.rowNB - 1 as rowNB,
            facebook_temp.spend
       from facebook_temp) as facebook_rowNB

WHERE facebook_rowNB.rowNB in (
        FLOOR((SELECT count(facebook_temp.rowNB) - 1 FROM facebook_temp) / 2),
        CEILING((SELECT count(facebook_temp.rowNB) - 1 FROM facebook_temp) / 2));

--check
SELECT PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY facebook_temp.spend) FROM facebook_temp;

--mysql
--SET @rowNB:= -1;
--SELECT SUM(spend) / COUNT(fb_ordered.rowNB)
--FROM(SELECT @rowNB := @rowNB + 1 as rowNB, spend
--     FROM fb
--       ORDER BY fb.spend)  fb_ordered
--WHERE fb_ordered.rowNB in (FLOOR(@rowNB / 2),  CEILING(@rowNB / 2));

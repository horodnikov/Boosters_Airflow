SELECT facebook_group.spend
FROM (SELECT facebook.spend, count(*) as Nb_duplicates
        FROM facebook
        WHERE EXTRACT(MONTH FROM facebook.date) = 08
        GROUP BY spend
      ) as facebook_group

JOIN (SELECT facebook.spend as spend_max, count(*) as Nb
        FROM facebook
        WHERE EXTRACT(MONTH FROM fb.date) = 08
        GROUP BY spend
        ORDER BY Nb DESC lIMIT 1
        ) facebook_group_max

ON facebook_group.Nb_duplicates = facebook_group_max.Nb

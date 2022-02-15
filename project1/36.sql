SELECT name
FROM Pokemon LEFT OUTER JOIN Evolution ON id = before_id 
WHERE type = 'water' AND after_id IS NULL
ORDER BY name;

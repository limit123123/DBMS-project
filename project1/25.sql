SELECT name
FROM Pokemon
WHERE id IN (
	SELECT e1.after_id second
	FROM Evolution e1 LEFT OUTER JOIN Evolution e2 ON e1.after_id = e2.before_id 
	WHERE e1.before_id NOT IN (
		SELECT e3.after_id
		FROM Evolution e3 LEFT OUTER JOIN Evolution e4 ON e3.after_id = e4.before_id) 
	)
ORDER BY name;
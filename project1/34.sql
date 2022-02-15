SELECT name
FROM Pokemon
WHERE id = (
  SELECT Third
  FROM (SELECT e1.before_id First, e2.after_id Third FROM Evolution e1 LEFT OUTER JOIN Evolution e2 ON e1.after_id = e2.before_id) tb1
  WHERE tb1.First = ( SELECT id FROM Pokemon WHERE name = 'Charmander' ));
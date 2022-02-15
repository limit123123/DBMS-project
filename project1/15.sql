SELECT Trainer.name
FROM Trainer JOIN CatchedPokemon ON Trainer.id = owner_id
WHERE hometown = 'Sangnok city' AND pid IN 
( SELECT pid FROM CatchedPokemon JOIN Pokemon ON pid = Pokemon.id 
 WHERE name LIKE 'p%' )
ORDER BY Trainer.name;
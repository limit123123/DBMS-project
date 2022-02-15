SELECT name 
FROM Pokemon
WHERE id NOT IN ( SELECT pid FROM CatchedPokemon)
ORDER BY name;
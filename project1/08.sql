SELECT COUNT(*)
FROM CatchedPokemon, Pokemon
WHERE CatchedPokemon.pid = Pokemon.id
GROUP BY Pokemon.type
ORDER BY Pokemon.type;
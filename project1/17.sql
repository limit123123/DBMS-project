SELECT AVG(level)
FROM CatchedPokemon, Pokemon
WHERE pid = Pokemon.id AND type = 'water';
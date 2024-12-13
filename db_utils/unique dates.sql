SELECT DISTINCT DATE(timestamp) AS unique_date
FROM public.player_shots_ou
ORDER BY unique_date ASC;
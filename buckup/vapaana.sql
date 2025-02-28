 SELECT rekisterinumero, merkki, malli, henkilomaara
   FROM public.auto
  WHERE rekisterinumero NOT IN ( 
	 SELECT rekisterinumero
           FROM ajossa)
  ORDER BY rekisterinumero;
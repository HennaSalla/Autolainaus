 SELECT rekisterinumero
   FROM public.lainaus
  WHERE palautus IS NULL
  ORDER BY rekisterinumero;
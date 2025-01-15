-- Luodaan person-taulu ja sen sarakeet 

CREATE TABLE public.person
(
    idnumero SERIAL PRIMARY KEY,
    etunimi CHARACTER VARYING NOT NULL,
    sukunimi CHARACTER VARYING NOT NULL
);
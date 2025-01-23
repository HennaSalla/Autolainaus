-- Table: public.person

-- DROP TABLE IF EXISTS public.person;

CREATE TABLE IF NOT EXISTS public.person
(
    idnumero integer NOT NULL DEFAULT nextval('person_idnumero_seq'::regclass),
    etunimi character varying(30) COLLATE pg_catalog."default" NOT NULL,
    sukunimi character varying(30) COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT person_pkey PRIMARY KEY (idnumero)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.person
    OWNER to postgres;
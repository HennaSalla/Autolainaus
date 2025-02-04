-- Luo autolainaustietokannan taulut ja laskurit

CREATE TABLE auto (
                rekisterinumero VARCHAR(7) NOT NULL,
                merkki VARCHAR(30) NOT NULL,
                malli VARCHAR(20) NOT NULL,
                vuosimalli CHAR(4) NOT NULL,
                henkilomaara INT,
                CONSTRAINT auto_pk PRIMARY KEY (rekisterinumero)
);
COMMENT ON TABLE auto IS 'Ajoneuvon perustiedot';


CREATE TABLE ryhma (
                ryhma VARCHAR(50) NOT NULL,
                vastuuhenkilo VARCHAR(50),
                CONSTRAINT ryhma_pk PRIMARY KEY (ryhma)
);
COMMENT ON TABLE ryhma IS 'Opiskelijan luokka';
COMMENT ON COLUMN ryhma.ryhma IS 'Ryhmän nimi, esim. auto22B tai henkilökunta';
COMMENT ON COLUMN ryhma.vastuuhenkilo IS 'Vastuuopettaja tai lähiesimies';


CREATE TABLE lainaaja (
                hetu CHAR(11) NOT NULL,
                sahkoposti VARCHAR(30),
                etunimi VARCHAR(50) NOT NULL,
                sukunimi VARCHAR(50) NOT NULL,
                ryhma VARCHAR(50) NOT NULL,
                ajokorttiluokka VARCHAR(6) NOT NULL,
                CONSTRAINT lainaaja_pk PRIMARY KEY (hetu)
);
COMMENT ON TABLE lainaaja IS 'Lainaajan (opiskelija tai ope) perustiedot';
COMMENT ON COLUMN lainaaja.hetu IS 'Kansallinen henkiltunnus';
COMMENT ON COLUMN lainaaja.sahkoposti IS 'Rasekon sähköpostiosoite';
COMMENT ON COLUMN lainaaja.ryhma IS 'Ryhmän nimi, esim. auto22B tai henkilökunta';
COMMENT ON COLUMN lainaaja.ajokorttiluokka IS 'Esim AB tai ABCE';


CREATE SEQUENCE lainaus_lainausnumero_seq;

CREATE TABLE lainaus (
                lainausnumero INTEGER NOT NULL DEFAULT nextval('lainaus_lainausnumero_seq'),
                hetu CHAR(11) NOT NULL,
                rekisterinumero VARCHAR(7) NOT NULL,
                lainausaika TIMESTAMP NOT NULL,
                palautus TIMESTAMP,
                CONSTRAINT lainaus_pk PRIMARY KEY (lainausnumero)
);
COMMENT ON TABLE lainaus IS 'Lainaustapahtuman tiedot';
COMMENT ON COLUMN lainaus.lainausnumero IS 'Lainaustapahtumalle automaattisesti annettava juokseva numero';
COMMENT ON COLUMN lainaus.hetu IS 'Kansallinen henkiltunnus';
COMMENT ON COLUMN lainaus.lainausaika IS 'Päivämäära ja kellonaika, kun auto on otettu lainaan';
COMMENT ON COLUMN lainaus.palautus IS 'Palautuksen päivä ja kellonaika';


ALTER SEQUENCE lainaus_lainausnumero_seq OWNED BY lainaus.lainausnumero;

ALTER TABLE lainaus ADD CONSTRAINT auto_lainaus_fk
FOREIGN KEY (rekisterinumero)
REFERENCES auto (rekisterinumero)
ON DELETE NO ACTION
ON UPDATE NO ACTION
NOT DEFERRABLE;

ALTER TABLE lainaaja ADD CONSTRAINT ryhma_lainaaja_fk
FOREIGN KEY (ryhma)
REFERENCES ryhma (ryhma)
ON DELETE NO ACTION
ON UPDATE NO ACTION
NOT DEFERRABLE;

ALTER TABLE lainaus ADD CONSTRAINT lainaaja_lainaus_fk
FOREIGN KEY (hetu)
REFERENCES lainaaja (hetu)
ON DELETE NO ACTION
ON UPDATE NO ACTION
NOT DEFERRABLE;

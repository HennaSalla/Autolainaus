--Database example data

INSERT INTO public.ryhma (ryhma, vastuuhenkilo) VALUES ('Auto22', 'Sami Nikkilä');
INSERT INTO public.ryhma (ryhma, vastuuhenkilo) VALUES ('Autosähkö24', 'Marko Hahto');
INSERT INTO public.ryhma (ryhma, vastuuhenkilo) VALUES ('Auto23A', 'Harri Kauppi');

INSERT INTO public.auto (rekisterinumero, merkki, malli, vuosimalli, henkilomaara) VALUES ('XSE-778', 'Toyota', 'bz4x', '2024', 5);
INSERT INTO public.auto (rekisterinumero, merkki, malli, vuosimalli, henkilomaara) VALUES ('AM-15', 'Renault', 'Megane', '2022', 5);

INSERT INTO public.lainaaja (hetu, sahkoposti, etunimi, sukunimi, ryhma, ajokorttiluokka) VALUES ('130728-478N', '12345@edu.raseko.fi', 'Jakke', 'Jäynä', 'Auto22', 'AB');
INSERT INTO public.lainaaja (hetu, sahkoposti, etunimi, sukunimi, ryhma, ajokorttiluokka) VALUES ('160201A145X', '55577@edu.raseko.fi', 'Jonne', 'Janttari', 'Auto22', 'A2');
INSERT INTO public.lainaaja (hetu, sahkoposti, etunimi, sukunimi, ryhma, ajokorttiluokka) VALUES ('181203A543R', '65643@edu.raseko.fi', 'Tuittu', 'Kiukkunen', 'Autosähkö24', 'ABEC');
INSERT INTO public.lainaaja (hetu, sahkoposti, etunimi, sukunimi, ryhma, ajokorttiluokka) VALUES ('150498-169A', '32323@edu.raseko.fi', 'Herman', 'Hoikka', 'Auto22', 'ABD');
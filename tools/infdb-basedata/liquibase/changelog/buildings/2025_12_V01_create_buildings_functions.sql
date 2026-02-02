--liquibase formatted sql
--changeset marvin.huang:1.0.1.1 labels:infdb-basedata,infdb-basedata-buildings endDelimiter:@@ rollbackEndDelimiter:@@
-- CreateFunction rollback is not automatically supported and requires custom rollback (Drop)
CREATE OR REPLACE FUNCTION ${output_schema}.classify_building_use(funktion TEXT)
    RETURNS TEXT AS
$$
BEGIN
    CASE funktion
    -- Adopted from https://repository.gdi-de.org/schemas/adv/citygml/Codelisten/BuildingFunctionTypeAdV.xml
    WHEN '31001_1000' THEN RETURN 'Residential'; -- Wohngebäude
    WHEN '31001_1010' THEN RETURN 'Residential'; -- Wohnhaus
    WHEN '31001_1020' THEN RETURN 'Residential'; -- Wohnheim
    WHEN '31001_1021' THEN RETURN 'Residential'; -- Kinderheim
    WHEN '31001_1022' THEN RETURN 'Residential'; -- Seniorenheim
    WHEN '31001_1023' THEN RETURN 'Residential'; -- Schwesternwohnheim
    WHEN '31001_1024' THEN RETURN 'Residential'; -- Studenten-, Schülerwohnheim
    WHEN '31001_1025' THEN RETURN 'Residential'; -- Schullandheim

    WHEN '31001_1100' THEN RETURN 'Residential'; -- Gemischt genutztes Gebäude mit Wohnen
    WHEN '31001_1110' THEN RETURN 'Residential'; -- Wohngebäude mit Gemeinbedarf
    WHEN '31001_1120' THEN RETURN 'Residential'; -- Wohngebäude mit Handel und Dienstleistungen
    WHEN '31001_1121' THEN RETURN 'Residential'; -- Wohn- und Verwaltungsgebäude
    WHEN '31001_1122' THEN RETURN 'Residential'; -- Wohn- und Bürogebäude
    WHEN '31001_1123' THEN RETURN 'Residential'; -- Wohn- und Geschäftsgebäude
    WHEN '31001_1130' THEN RETURN 'Residential'; -- Wohngebäude mit Gewerbe und Industrie
    WHEN '31001_1131' THEN RETURN 'Residential'; -- Wohn- und Betriebsgebäude

    WHEN '31001_1210' THEN RETURN 'Residential'; -- Land- und forstwirtschaftliches Wohngebäude
    WHEN '31001_1220' THEN RETURN 'Residential'; -- Land- und forstwirtschaftliches Wohn- und Betriebsgebäude
    WHEN '31001_1221' THEN RETURN 'Residential'; -- Bauernhaus
    WHEN '31001_1222' THEN RETURN 'Residential'; -- Wohn- und Wirtschaftsgebäude
    WHEN '31001_1223' THEN RETURN 'Residential'; -- Forsthaus

    WHEN '31001_1310' THEN RETURN 'Residential'; -- Gebäude zur Freizeitgestaltung
    WHEN '31001_1311' THEN RETURN 'Residential'; -- Ferienhaus
    WHEN '31001_1312' THEN RETURN 'Residential'; -- Wochenendhaus
    WHEN '31001_1313' THEN RETURN 'Residential'; -- Gartenhaus

    WHEN '31001_2000' THEN RETURN 'Commercial'; -- Gebäude für Wirtschaft oder Gewerbe
    WHEN '31001_2010' THEN RETURN 'Commercial'; -- Gebäude für Handel und Dienstleistungen
    WHEN '31001_2020' THEN RETURN 'Commercial'; -- Bürogebäude
    WHEN '31001_2030' THEN RETURN 'Commercial'; -- Kreditinstitut
    WHEN '31001_2040' THEN RETURN 'Commercial'; -- Versicherung
    WHEN '31001_2050' THEN RETURN 'Commercial'; -- Geschäftsgebäude
    WHEN '31001_2051' THEN RETURN 'Commercial'; -- Kaufhaus
    WHEN '31001_2052' THEN RETURN 'Commercial'; -- Einkaufszentrum
    WHEN '31001_2053' THEN RETURN 'Commercial'; -- Markthalle
    WHEN '31001_2054' THEN RETURN 'Commercial'; -- Laden
    WHEN '31001_2055' THEN RETURN 'Commercial'; -- Kiosk
    WHEN '31001_2056' THEN RETURN 'Commercial'; -- Apotheke
    WHEN '31001_2060' THEN RETURN 'Commercial'; -- Messehalle

    WHEN '31001_2070' THEN RETURN 'Commercial'; -- Gebäude für Beherbergung
    WHEN '31001_2071' THEN RETURN 'Commercial'; -- Hotel, Motel, Pension
    WHEN '31001_2072' THEN RETURN 'Commercial'; -- Jugendherberge
    WHEN '31001_2073' THEN RETURN 'Commercial'; -- Hütte (mit Übernachtungsmöglichkeit)
    WHEN '31001_2074' THEN RETURN 'Commercial'; -- Campingplatzgebäude

    WHEN '31001_2080' THEN RETURN 'Commercial'; -- Gebäude für Bewirtung
    WHEN '31001_2081' THEN RETURN 'Commercial'; -- Gaststätte, Restaurant
    WHEN '31001_2082' THEN RETURN 'Commercial'; -- Hütte (ohne Übernachtungsmöglichkeit)
    WHEN '31001_2083' THEN RETURN 'Commercial'; -- Kantine

    WHEN '31001_2090' THEN RETURN 'Commercial'; -- Freizeit- und Vergnügungsstätte
    WHEN '31001_2091' THEN RETURN 'Commercial'; -- Festsaal
    WHEN '31001_2092' THEN RETURN 'Commercial'; -- Kino
    WHEN '31001_2093' THEN RETURN 'Commercial'; -- Kegel-, Bowlinghalle
    WHEN '31001_2094' THEN RETURN 'Commercial'; -- Spielkasino
    WHEN '31001_2095' THEN RETURN 'Commercial'; -- Spielhalle

    WHEN '31001_2100' THEN RETURN 'Commercial'; -- Gebäude für Gewerbe und Industrie
    WHEN '31001_2110' THEN RETURN 'Commercial'; -- Produktionsgebäude
    WHEN '31001_2111' THEN RETURN 'Commercial'; -- Fabrik
    WHEN '31001_2112' THEN RETURN 'Commercial'; -- Betriebsgebäude
    WHEN '31001_2113' THEN RETURN 'Commercial'; -- Brauerei
    WHEN '31001_2114' THEN RETURN 'Commercial'; -- Brennerei
    WHEN '31001_2120' THEN RETURN 'Commercial'; -- Werkstatt
    WHEN '31001_2121' THEN RETURN 'Commercial'; -- Sägewerk
    WHEN '31001_2130' THEN RETURN 'Commercial'; -- Tankstelle
    WHEN '31001_2131' THEN RETURN 'Commercial'; -- Waschstraße, Waschanlage, Waschhalle
    WHEN '31001_2140' THEN RETURN 'Commercial'; -- Gebäude für Vorratshaltung
    WHEN '31001_2141' THEN RETURN 'Commercial'; -- Kühlhaus
    WHEN '31001_2142' THEN RETURN 'Commercial'; -- Speichergebäude
    WHEN '31001_2143' THEN RETURN 'Commercial'; -- Lagerhalle, Lagerschuppen, Lagerhaus
    WHEN '31001_2150' THEN RETURN 'Commercial'; -- Speditionsgebäude
    WHEN '31001_2160' THEN RETURN 'Commercial'; -- Gebäude für Forschungszwecke
    WHEN '31001_2170' THEN RETURN 'Commercial'; -- Gebäude für Grundstoffgewinnung
    WHEN '31001_2171' THEN RETURN 'Commercial'; -- Bergwerk
    WHEN '31001_2172' THEN RETURN 'Commercial'; -- Saline
    WHEN '31001_2180' THEN RETURN 'Commercial'; -- Gebäude für betriebliche Sozialeinrichtung

    WHEN '31001_2200' THEN RETURN 'Commercial'; -- Sonstiges Gebäude für Gewerbe und Industrie
    WHEN '31001_2210' THEN RETURN 'Commercial'; -- Mühle
    WHEN '31001_2211' THEN RETURN 'Commercial'; -- Windmühle
    WHEN '31001_2212' THEN RETURN 'Commercial'; -- Wassermühle
    WHEN '31001_2213' THEN RETURN 'Public';      -- Schöpfwerk
    WHEN '31001_2220' THEN RETURN 'Public';      -- Wetterstation

    WHEN '31001_2310' THEN RETURN 'Residential'; -- Gebäude für Handel und Dienstleistung mit Wohnen
    WHEN '31001_2320' THEN RETURN 'Residential'; -- Gebäude für Gewerbe und Industrie mit Wohnen

    WHEN '31001_2400' THEN RETURN 'Public'; -- Betriebsgebäude zu Verkehrsanlagen (allgemein)
    WHEN '31001_2410' THEN RETURN 'Public'; -- Betriebsgebäude für Straßenverkehr
    WHEN '31001_2411' THEN RETURN 'Public'; -- Straßenmeisterei
    WHEN '31001_2412' THEN RETURN 'Public'; -- Wartehalle
    WHEN '31001_2420' THEN RETURN 'Public'; -- Betriebsgebäude für Schienenverkehr
    WHEN '31001_2421' THEN RETURN 'Public'; -- Bahnwärterhaus
    WHEN '31001_2422' THEN RETURN 'Public'; -- Lokschuppen, Wagenhalle
    WHEN '31001_2423' THEN RETURN 'Public'; -- Stellwerk, Blockstelle
    WHEN '31001_2424' THEN RETURN 'Public'; -- Betriebsgebäude des Güterbahnhofs
    WHEN '31001_2430' THEN RETURN 'Public'; -- Betriebsgebäude für Flugverkehr
    WHEN '31001_2431' THEN RETURN 'Public'; -- Flugzeughalle
    WHEN '31001_2440' THEN RETURN 'Public'; -- Betriebsgebäude für Schiffsverkehr
    WHEN '31001_2441' THEN RETURN 'Public'; -- Werft (Halle)
    WHEN '31001_2442' THEN RETURN 'Public'; -- Dock (Halle)
    WHEN '31001_2443' THEN RETURN 'Public'; -- Betriebsgebäude zur Schleuse
    WHEN '31001_2444' THEN RETURN 'Public'; -- Bootshaus
    WHEN '31001_2450' THEN RETURN 'Public'; -- Betriebsgebäude zur Seilbahn
    WHEN '31001_2451' THEN RETURN 'Public'; -- Spannwerk zur Drahtseilbahn
    WHEN '31001_2460' THEN RETURN 'Public'; -- Gebäude zum Parken
    WHEN '31001_2461' THEN RETURN 'Public'; -- Parkhaus
    WHEN '31001_2462' THEN RETURN 'Public'; -- Parkdeck
    WHEN '31001_2463' THEN RETURN 'Public'; -- Garage
    WHEN '31001_2464' THEN RETURN 'Public'; -- Fahrzeughalle
    WHEN '31001_2465' THEN RETURN 'Public'; -- Tiefgarage

    WHEN '31001_2500' THEN RETURN 'Public'; -- Gebäude zur Versorgung
    WHEN '31001_2501' THEN RETURN 'Public'; -- Gebäude zur Energieversorgung
    WHEN '31001_2510' THEN RETURN 'Public'; -- Gebäude zur Wasserversorgung
    WHEN '31001_2511' THEN RETURN 'Public'; -- Wasserwerk
    WHEN '31001_2512' THEN RETURN 'Public'; -- Pumpstation
    WHEN '31001_2513' THEN RETURN 'Public'; -- Wasserbehälter
    WHEN '31001_2520' THEN RETURN 'Public'; -- Gebäude zur Elektrizitätsversorgung
    WHEN '31001_2521' THEN RETURN 'Public'; -- Elektrizitätswerk
    WHEN '31001_2522' THEN RETURN 'Public'; -- Umspannwerk
    WHEN '31001_2523' THEN RETURN 'Public'; -- Umformer
    WHEN '31001_2527' THEN RETURN 'Public'; -- Reaktorgebäude
    WHEN '31001_2528' THEN RETURN 'Public'; -- Turbinenhaus
    WHEN '31001_2529' THEN RETURN 'Public'; -- Kesselhaus
    WHEN '31001_2540' THEN RETURN 'Public'; -- Gebäude für Fernmeldewesen
    WHEN '31001_2560' THEN RETURN 'Public'; -- Gebäude an unterirdischen Leitungen
    WHEN '31001_2570' THEN RETURN 'Public'; -- Gebäude zur Gasversorgung
    WHEN '31001_2571' THEN RETURN 'Public'; -- Gaswerk
    WHEN '31001_2580' THEN RETURN 'Public'; -- Heizwerk
    WHEN '31001_2590' THEN RETURN 'Public'; -- Gebäude zur Versorgungsanlage
    WHEN '31001_2591' THEN RETURN 'Public'; -- Pumpwerk (nicht für Wasserversorgung)

    WHEN '31001_2600' THEN RETURN 'Public'; -- Gebäude zur Entsorgung
    WHEN '31001_2610' THEN RETURN 'Public'; -- Gebäude zur Abwasserbeseitigung
    WHEN '31001_2611' THEN RETURN 'Public'; -- Gebäude der Kläranlage
    WHEN '31001_2612' THEN RETURN 'Public'; -- Toilette
    WHEN '31001_2620' THEN RETURN 'Public'; -- Gebäude zur Abfallbehandlung
    WHEN '31001_2621' THEN RETURN 'Public'; -- Müllbunker
    WHEN '31001_2622' THEN RETURN 'Public'; -- Gebäude zur Müllverbrennung
    WHEN '31001_2623' THEN RETURN 'Public'; -- Gebäude der Abfalldeponie

    WHEN '31001_2700' THEN RETURN 'Commercial'; -- Gebäude für Land- und Forstwirtschaft
    WHEN '31001_2720' THEN RETURN 'Commercial'; -- Land- und forstwirtschaftliches Betriebsgebäude
    WHEN '31001_2721' THEN RETURN 'Commercial'; -- Scheune
    WHEN '31001_2723' THEN RETURN 'Commercial'; -- Schuppen
    WHEN '31001_2724' THEN RETURN 'Commercial'; -- Stall
    WHEN '31001_2726' THEN RETURN 'Commercial'; -- Scheune und Stall
    WHEN '31001_2727' THEN RETURN 'Commercial'; -- Stall für Tiergroßhaltung
    WHEN '31001_2728' THEN RETURN 'Commercial'; -- Reithalle
    WHEN '31001_2729' THEN RETURN 'Commercial'; -- Wirtschaftsgebäude
    WHEN '31001_2732' THEN RETURN 'Commercial'; -- Almhütte
    WHEN '31001_2735' THEN RETURN 'Commercial'; -- Jagdhaus, Jagdhütte
    WHEN '31001_2740' THEN RETURN 'Commercial'; -- Treibhaus, Gewächshaus
    WHEN '31001_2741' THEN RETURN 'Commercial'; -- Treibhaus
    WHEN '31001_2742' THEN RETURN 'Commercial'; -- Gewächshaus, verschiebbar

    WHEN '31001_3000' THEN RETURN 'Public'; -- Gebäude für öffentliche Zwecke
    WHEN '31001_3010' THEN RETURN 'Public'; -- Verwaltungsgebäude
    WHEN '31001_3011' THEN RETURN 'Public'; -- Parlament
    WHEN '31001_3012' THEN RETURN 'Public'; -- Rathaus
    WHEN '31001_3013' THEN RETURN 'Public'; -- Post
    WHEN '31001_3014' THEN RETURN 'Public'; -- Zollamt
    WHEN '31001_3015' THEN RETURN 'Public'; -- Gericht
    WHEN '31001_3016' THEN RETURN 'Public'; -- Botschaft, Konsulat
    WHEN '31001_3017' THEN RETURN 'Public'; -- Kreisverwaltung
    WHEN '31001_3018' THEN RETURN 'Public'; -- Bezirksregierung
    WHEN '31001_3019' THEN RETURN 'Public'; -- Finanzamt

    WHEN '31001_3020' THEN RETURN 'Public'; -- Gebäude für Bildung und Forschung
    WHEN '31001_3021' THEN RETURN 'Public'; -- Allgemein bildende Schule
    WHEN '31001_3022' THEN RETURN 'Public'; -- Berufsbildende Schule
    WHEN '31001_3023' THEN RETURN 'Public'; -- Hochschulgebäude (Fachhochschule, Universität)
    WHEN '31001_3024' THEN RETURN 'Public'; -- Forschungsinstitut

    WHEN '31001_3030' THEN RETURN 'Public'; -- Gebäude für kulturelle Zwecke
    WHEN '31001_3031' THEN RETURN 'Public'; -- Schloss
    WHEN '31001_3032' THEN RETURN 'Public'; -- Theater, Oper
    WHEN '31001_3033' THEN RETURN 'Public'; -- Konzertgebäude
    WHEN '31001_3034' THEN RETURN 'Public'; -- Museum
    WHEN '31001_3035' THEN RETURN 'Public'; -- Rundfunk, Fernsehen
    WHEN '31001_3036' THEN RETURN 'Public'; -- Veranstaltungsgebäude
    WHEN '31001_3037' THEN RETURN 'Public'; -- Bibliothek, Bücherei
    WHEN '31001_3038' THEN RETURN 'Public'; -- Burg, Festung

    WHEN '31001_3040' THEN RETURN 'Public'; -- Gebäude für religiöse Zwecke
    WHEN '31001_3041' THEN RETURN 'Public'; -- Kirche
    WHEN '31001_3042' THEN RETURN 'Public'; -- Synagoge
    WHEN '31001_3043' THEN RETURN 'Public'; -- Kapelle
    WHEN '31001_3044' THEN RETURN 'Public'; -- Gemeindehaus
    WHEN '31001_3045' THEN RETURN 'Public'; -- Gotteshaus
    WHEN '31001_3046' THEN RETURN 'Public'; -- Moschee
    WHEN '31001_3047' THEN RETURN 'Public'; -- Tempel
    WHEN '31001_3048' THEN RETURN 'Public'; -- Kloster

    WHEN '31001_3050' THEN RETURN 'Public'; -- Gebäude für Gesundheitswesen
    WHEN '31001_3051' THEN RETURN 'Public'; -- Krankenhaus
    WHEN '31001_3052' THEN RETURN 'Public'; -- Heilanstalt, Pflegeanstalt, Pflegestation
    WHEN '31001_3053' THEN RETURN 'Public'; -- Ärztehaus, Poliklinik
    WHEN '31001_3054' THEN RETURN 'Public'; -- Rettungswache

    WHEN '31001_3060' THEN RETURN 'Public'; -- Gebäude für soziale Zwecke
    WHEN '31001_3061' THEN RETURN 'Public'; -- Jugendfreizeitheim
    WHEN '31001_3062' THEN RETURN 'Public'; -- Freizeit-, Vereinsheim, Dorfgemeinschafts-, Bürgerhaus
    WHEN '31001_3063' THEN RETURN 'Public'; -- Seniorenfreizeitstätte
    WHEN '31001_3064' THEN RETURN 'Public'; -- Obdachlosenheim
    WHEN '31001_3065' THEN RETURN 'Public'; -- Kinderkrippe, Kindergarten, Kindertagesstätte
    WHEN '31001_3066' THEN RETURN 'Public'; -- Asylbewerberheim

    WHEN '31001_3070' THEN RETURN 'Public'; -- Gebäude für Sicherheit und Ordnung
    WHEN '31001_3071' THEN RETURN 'Public'; -- Polizei
    WHEN '31001_3072' THEN RETURN 'Public'; -- Feuerwehr
    WHEN '31001_3073' THEN RETURN 'Public'; -- Kaserne
    WHEN '31001_3074' THEN RETURN 'Public'; -- Schutzbunker
    WHEN '31001_3075' THEN RETURN 'Public'; -- Justizvollzugsanstalt

    WHEN '31001_3080' THEN RETURN 'Public'; -- Friedhofsgebäude
    WHEN '31001_3081' THEN RETURN 'Public'; -- Trauerhalle
    WHEN '31001_3082' THEN RETURN 'Public'; -- Krematorium

    WHEN '31001_3090' THEN RETURN 'Public'; -- Empfangsgebäude
    WHEN '31001_3091' THEN RETURN 'Public'; -- Bahnhofsgebäude
    WHEN '31001_3092' THEN RETURN 'Public'; -- Flughafengebäude
    WHEN '31001_3094' THEN RETURN 'Public'; -- Gebäude zum U-Bahnhof
    WHEN '31001_3095' THEN RETURN 'Public'; -- Gebäude zum S-Bahnhof
    WHEN '31001_3097' THEN RETURN 'Public'; -- Gebäude zum Busbahnhof
    WHEN '31001_3098' THEN RETURN 'Public'; -- Empfangsgebäude Schifffahrt

    WHEN '31001_3100' THEN RETURN 'Public'; -- Gebäude für öffentliche Zwecke mit Wohnen

    WHEN '31001_3200' THEN RETURN 'Public'; -- Gebäude für Erholungszwecke
    WHEN '31001_3210' THEN RETURN 'Public'; -- Gebäude für Sportzwecke
    WHEN '31001_3211' THEN RETURN 'Public'; -- Sport-, Turnhalle
    WHEN '31001_3212' THEN RETURN 'Public'; -- Gebäude zum Sportplatz
    WHEN '31001_3220' THEN RETURN 'Public'; -- Badegebäude
    WHEN '31001_3221' THEN RETURN 'Public'; -- Hallenbad
    WHEN '31001_3222' THEN RETURN 'Public'; -- Gebäude im Freibad
    WHEN '31001_3230' THEN RETURN 'Public'; -- Gebäude im Stadion
    WHEN '31001_3240' THEN RETURN 'Public'; -- Gebäude für Kurbetrieb
    WHEN '31001_3241' THEN RETURN 'Public'; -- Badegebäude für medizinische Zwecke
    WHEN '31001_3242' THEN RETURN 'Public'; -- Sanatorium
    WHEN '31001_3260' THEN RETURN 'Public'; -- Gebäude im Zoo
    WHEN '31001_3261' THEN RETURN 'Public'; -- Empfangsgebäude des Zoos
    WHEN '31001_3262' THEN RETURN 'Public'; -- Aquarium, Terrarium, Voliere
    WHEN '31001_3263' THEN RETURN 'Public'; -- Tierschauhaus
    WHEN '31001_3264' THEN RETURN 'Public'; -- Stall im Zoo
    WHEN '31001_3270' THEN RETURN 'Public'; -- Gebäude im botanischen Garten
    WHEN '31001_3271' THEN RETURN 'Public'; -- Empfangsgebäude des botanischen Gartens
    WHEN '31001_3272' THEN RETURN 'Public'; -- Gewächshaus (Botanik)
    WHEN '31001_3273' THEN RETURN 'Public'; -- Pflanzenschauhaus
    WHEN '31001_3280' THEN RETURN 'Public'; -- Gebäude für andere Erholungseinrichtung
    WHEN '31001_3281' THEN RETURN 'Public'; -- Schutzhütte
    WHEN '31001_3290' THEN RETURN 'Public'; -- Touristisches Informationszentrum

    WHEN '31001_9998' THEN RETURN 'Public'; -- Nach Quellenlage nicht zu spezifizieren
        ELSE RAISE EXCEPTION 'Unknown building function code: %', funktion;
        END CASE;
END;
$$ LANGUAGE plpgsql IMMUTABLE
                    STRICT;
@@
--rollback DROP FUNCTION IF EXISTS ${output_schema}.classify_building_use(TEXT);
@@

--changeset marvin.huang:1.0.1.2 labels:infdb-basedata,infdb-basedata-buildings endDelimiter:@@ rollbackEndDelimiter:@@
-- Allow replace if it database already has it
CREATE OR REPLACE FUNCTION ${output_schema}.assign_weighted_year(
    vor1919 double precision,
    a1919bis1948 double precision,
    a1949bis1978 double precision,
    a1979bis1990 double precision,
    a1991bis2000 double precision,
    a2001bis2010 double precision,
    a2011bis2019 double precision,
    a2020undspaeter double precision,
    r double precision
) RETURNS TEXT AS $$
DECLARE
    total NUMERIC;
    cumulative NUMERIC;
BEGIN
    total := vor1919 + a1919bis1948 + a1949bis1978 + a1979bis1990 + a1991bis2000 + a2001bis2010 + a2011bis2019 + a2020undspaeter;

    IF total <= 0 THEN
        RETURN NULL;
    END IF;

    cumulative := 0;

    cumulative := cumulative + vor1919;
    IF r <= cumulative / total THEN
        RETURN '-1919';
    END IF;

    cumulative := cumulative + a1919bis1948;
    IF r <= cumulative / total THEN
        RETURN '1919-1948';
    END IF;

    cumulative := cumulative + a1949bis1978;
    IF r <= cumulative / total THEN
        RETURN '1949-1978';
    END IF;

    cumulative := cumulative + a1979bis1990;
    IF r <= cumulative / total THEN
        RETURN '1979-1990';
    END IF;

    cumulative := cumulative + a1991bis2000;
    IF r <= cumulative / total THEN
        RETURN '1991-2000';
    END IF;

    cumulative := cumulative + a2001bis2010;
    IF r <= cumulative / total THEN
        RETURN '2001-2010';
    END IF;

    cumulative := cumulative + a2011bis2019;
    IF r <= cumulative / total THEN
        RETURN '2011-2019';
    END IF;

    -- If we reach here, assign to the last period
    RETURN '2020-';
END;
$$ LANGUAGE plpgsql;
@@
--rollback DROP FUNCTION IF EXISTS ${output_schema}.assign_weighted_year(double precision, double precision, double precision, double precision, double precision, double precision, double precision, double precision, double precision);
@@
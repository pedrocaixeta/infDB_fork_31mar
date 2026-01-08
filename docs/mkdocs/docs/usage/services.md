---
icon: material/cogs
---
The infDB platform provides a suite of essential services designed to facilitate database operation and administration, data handling and visualization, and connectivity. Each preconfigured service can be activated individually to tailor the environment to your specific requirements. This section provides a brief description and configuration options for each available service.

![alt text](services.png)

## Overview:

- [infdb-db](#infdb-db-material-database): Core PostgreSQL database with PostGIS, timescaledb, and pgrouting extensions; handles all central storage and queries.
- [infdb-importer](#infdb-importer-material-cloud-download): Automates the ingestion, structuring, and integration of external open data formats into the platform.
- [pgAdmin](https://www.pgadmin.org/): Web UI for inspecting schemas, running SQL, managing roles; auto-configured credentials.
- [FastAPI](https://fastapi.tiangolo.com/): REST endpoints (/city, /weather) with OpenAPI docs and validated access to 3D, geospatial, and time-series data.
- [Jupyter](https://jupyter.org/): Notebook environment (dependencies and env vars preloaded) for exploratory queries, ETL prototypes, reproducible analysis.
- [QWC2](https://github.com/qwc-services/qwc2): Web mapping client for 2D/3D visualization, layer styling, spatial inspection, quick dataset validation.
- [PostgREST](https://postgrest.org/): Auto-generated REST API over PostgreSQL schemas (tables, views, RPC) using DB roles for auth; rapid, lightweight data access without extra backend code.
- [pygeoapi](https://pygeoapi.io/): OGC API (Features/Coverages/Processes) server exposing PostGIS data via standards-based JSON & HTML endpoints for interoperable geospatial discovery and querying.

## infdb-db :material-database:
The core service **infdb-db** hosts the PostgreSQL database with extensions for geospatial, time series and graph data, serving as the central database within the platform. It handles data storage, retrieval, and management, ensuring integrity and high availability for connected services and tools.

The configuration:
``` bash title=".env"
# ==============================================================================
# SERVICE ACTIVATION
# ==============================================================================
# Select profiles to activate
COMPOSE_PROFILES=core,...  # (3)

# ==============================================================================
# POSTGRESQL DATABASE (Core Service)
# ==============================================================================
# Profile: core

# Database name
SERVICES_POSTGRES_DB=infdb  # (4)

# Database credentials
SERVICES_POSTGRES_USER=infdb_user   # (5)
SERVICES_POSTGRES_PASSWORD=infdb    # (6)

# Host:Port address from which a container is able to reach the Postgres database
SERVICES_POSTGRES_HOST=host.docker.internal # (7)
SERVICES_POSTGRES_EXPOSED_PORT=54328    # (8)

# EPSG code for spatial reference system (25832 = ETRS89 / UTM zone 32N)
SERVICES_POSTGRES_EPSG=25832    # (9)
```

1. a
2. b
3. Activate database service
4. name of base postgres database
5. Admin user of postgres database
6. Admin password of postgres database
7. internal parameter for docker experts
8. Port that exposes outside of docker and used to communicate with other applications.
9. Default coordinate reference system (CRS) for postgres database


## infdb-importer :material-cloud-download:
The infdb-importer service facilitates the ingestion of external data into the infDB platform. It automates the process of importing various open data formats, ensuring that new datasets are properly structured and integrated into the core database for immediate use.

The configuration:
``` bash title=".env"
# ==============================================================================
# SERVICE ACTIVATION
# ==============================================================================
# Select profiles to activate
COMPOSE_PROFILES=...,opendata,...  # (1)

# ==============================================================================
# DATA IMPORTER AND LOADER (infdb-importer)
# ==============================================================================
# Profile: opendata
# Path to the yaml configuration file for the infdb-loader "configs/config-infdb-loader.yml"
```

1. Profile "opendata" must be within the list to activate the infdb-importer service

The imported opendata sources can be configured in a YAML file. The following sources are available on a municipality level (AGS) for North Rhine-Westphalia and Bavaria:

- Building data (LOD2) for NRW and BY
- Statistical data from Zenus 2022 and 2010
- Building topology by TABULA
- Weather and time series data by Openmeteo
- Administrative areas by BKG
- Postcodes from OpenStreetMap

The configuration:

``` yaml title="configs/config-infdb-loader.yml"
# Configuration file for infdb-loader
#
# This configuration file contains tool-specific settings and database connection parameters.
#
# Database Connection Parameters:
# - Parameters set to 'None' will be automatically replaced with values from the central
#   configuration file specified in 'config-infdb' (config-infdb.yml by default)
# - This approach allows you to run locally using centralized database settings
# - To connect to a remote infdb instance, replace 'None' values with your specific
#   connection parameters (user, password, db, host, exposed_port, epsg)
#
infdb-loader:
    name: "forchheim"  # (1)
    scope:  # (2)
        # - "09162000"  # Munich
        - "09780139"  # Sonthofen
        # - "09780116"  # Bolsterlang
        # - "09162000" # M
        # - "09185149" # ND
        # - "09474126" # FO
        # - "09261000" # LA
    multiproccesing: 
        status: not-active
        max_cores: 2    # max cores since of memory limitations to 2
    config-infdb: "config-infdb.yml" # (3)
    path:
        opendata: "opendata/"
        processed: "{infdb-loader/name}"
    logging:
        path: "infdb-loader.log"
        level: "INFO" # ERROR, WARNING, INFO, DEBUG
    hosts:
        postgres:
            user: None # (4)
            password: None
            db: None
            host: None  # change to external IP if not running on local machine
            exposed_port: None
            epsg: None # 3035 (Europe)
        webdav:
            username: infdb
            access_token: "letdown subscribe lily catchable landmine sphinx"
    sources:
        package:
            status: active # (5)
            url: http://ds1.need.energy:8123/opendata-neuburg-demo.zip
            path:
                base: "{infdb-loader/path/base}"
                processed: "{infdb-loader/path/opendata}"
        need:
            status: not-active
            host: "ds1.need.energy"
            database: need
            port: 5431
            user: postgres
            password: postgres
            schema_input: need
            path_dump: "{infdb-loader/path/opendata}/need/"

        opendata_bavaria:
            status: active
            schema: opendata
            prefix: bavaria
            path: 
                base: "{infdb-loader/path/opendata}/opendata_bavaria/"
            datasets:
                gelaendemodell_1m:
                    status: not-active
                    name: Geländemodell Bayern 1m
                    table_name: gelaendemodell_1m
                    # - Lower values (1-5m): High detail, large files, slower queries
                    # - Medium values (10-20m): Good balance for most applications
                    # - Higher values (50-100m): Coarse terrain, small files, fast queries
                    target_resolution: 10.0
                    url: "https://geodaten.bayern.de/odd/a/dgm/dgm1/meta/metalink/#scope.meta4"
                building_lod2:
                    status: active
                    name: 3D-Gebäudemodelle (LoD2)
                    table_name: building_lod2
                    import-mode: skip
                    url: "https://geodaten.bayern.de/odd/a/lod2/citygml/meta/metalink/#scope.meta4" 
                tatsaechliche_nutzung:
                    status: not-active
                    name: Tatsächliche Nutzung (TN) 
                    table_name: tatsaechliche_nutzung
                    url: "https://geodaten.bayern.de/odd/m/3/daten/tn/Nutzung_kreis.gpkg"
        lod2-nrw:
            status: not-active
            import-mode: skip # delete
            url:
                - "http://ds1.need.energy:8123/3d-gm_lod2_kacheln.zip"    #scope placeholder for AGS
            path:
                lod2: "{infdb-loader/path/opendata}/lod2-nrw/"
                gml: "{infdb-loader/path/opendata}/lod2-nrw/{infdb-loader/name}"

        bkg:
            status: active
            path:
                base: "{infdb-loader/path/opendata}/bkg/"
                zip: "{infdb-loader/sources/bkg/path/base}/zip/"
                unzip: "{infdb-loader/sources/bkg/path/base}/unzip/"
                processed: "{infdb-loader/path/processed}/bkg/"
            prefix: bkg
            schema: opendata
            vg5000:
                url: "https://daten.gdz.bkg.bund.de/produkte/vg/vg5000_1231/aktuell/vg5000_12-31.utm32s.gpkg.ebenen.zip"
                layer:
                    - "vg5000_gem"
                    - "vg5000_krs"
                    - "vg5000_lan"
                    - "vg5000_li"
                    - "vg5000_rbz"
                    - "vg5000_sta"
                    - "vg5000_vwg"
            nuts:
                url: "https://daten.gdz.bkg.bund.de/produkte/vg/nuts250_1231/aktuell/nuts250_12-31.utm32s.gpkg.zip"
                layer:
                    - "nuts250_n1"
                    - "nuts250_n2"
                    - "nuts250_n3"
            geogitter:
                table_name: grid_cells
                resolutions:
                     - 100m
                     - 1km
                     - 10km

        basemap:
            status: active
            url: "https://basemap.de/dienste/opendata/basisviews/"
            ending: ".gpkg"
            filter:
                - by # Bavaria
                - nw # North Rhine-Westphalia
            path:
                base: "{infdb-loader/path/opendata}/basemap/"
                processed: "{infdb-loader/path/processed}/basemap/"
            schema: opendata
            prefix: basemap
            layer:
                # - barrierenlinie
                # - bauwerksflaeche
                # - bauwerkslinie
                # - bauwerkspunkt
                # - besondere_flaeche
                # - besondere_linie
                # - besonderer_punkt
                # - gewaesserflaeche
                # - gewaesserlinie
                # - gewaesserpunkt
                # - grenze_flaeche
                # - grenze_linie
                # - grenze_punkt
                # - historische_flaeche
                # - historische_linie
                # - historischer_punkt
                # - name_flaeche
                # - name_punkt
                # - reliefflaeche
                # - relieflinie
                # - reliefpunkt
                # - siedlungsflaeche
                # - vegetationsflaeche
                # - vegetationslinie
                # - vegetationspunkt
                # - verkehrsflaeche
                - verkehrslinie
                # - verkehrspunkt
                # - versorgungslinie
                # - versorgungspunkt
                # - weitere_nutzung_flaeche
        plz:
            status: active
            url: "https://cloud.ocd.need.energy/remote.php/dav/spaces/cd8ca458-980c-41e6-9cd2-5137f78e039d$ea73c208-4fae-4ef3-a13e-7cb01a5e657a/plz-5stellig.geojson"
            protocol: webdav
            username: "infdb"
            access_token: "letdown subscribe lily catchable landmine sphinx"
            path:
                base: "{infdb-loader/path/opendata}/plz/"
                processed: "{infdb-loader/path/processed}/plz/"
            schema: opendata
            prefix: postcodes
            layer:
                - plz-5stellig
        tabula:
            status: active
            url:
                - "https://raw.githubusercontent.com/RWTH-EBC/TEASER/refs/heads/main/teaser/data/input/inputdata/TypeElements_TABULA_DE.json"
                - "https://raw.githubusercontent.com/RWTH-EBC/TEASER/refs/heads/main/teaser/data/input/inputdata/MaterialTemplates.json"
            path:
                base: "{infdb-loader/path/opendata}/tabula/"
            schema: opendata
            prefix: tabula
        zensus_2022:
            status: active
            resolutions:
                - 10km
                # - 1km
                - 100m
            years:
                - 2022
                # - 2011
            path:
                base: "{infdb-loader/path/opendata}/zensus/"
                zip: "{infdb-loader/sources/zensus_2022/path/base}/zip/"
                unzip: "{infdb-loader/sources/zensus_2022/path/base}/unzip/"
                processed: "{infdb-loader/path/processed}/zensus_2022/"
            url: "https://www.zensus2022.de/DE/Ergebnisse-des-Zensus/_inhalt.html"
            schema: opendata
            prefix: zensus
            save_local: not-active
            datasets:
                - name: Bevoelkerungszahl
                  status: active
                  table_name: bevoelkerungszahl
                  year: 2022
                  url: https://www.destatis.de/static/DE/zensus/gitterdaten/Zensus2022_Bevoelkerungszahl.zip

                - name: Deutsche Staatsangehoerige 18+
                  status: not-active
                  table_name: deutsche_staatsangehoerige_18plus
                  year: 2022
                  url: https://www.destatis.de/static/DE/zensus/gitterdaten/Deutsche_Staatsangehoerige_ab_18_Jahren.zip

                - name: Auslaenderanteil
                  status: not-active
                  table_name: auslaenderanteil
                  year: 2022
                  url: https://www.destatis.de/static/DE/zensus/gitterdaten/Auslaenderanteil_in_Gitterzellen.zip

                - name: Auslaenderanteil 18+
                  status: not-active
                  table_name: auslaenderanteil_18plus
                  year: 2022
                  url: https://www.destatis.de/static/DE/zensus/gitterdaten/Auslaenderanteil_ab_18_Jahren.zip

                - name: Geburtsland Gruppen
                  status: not-active
                  table_name: geburtsland_gruppen
                  year: 2022
                  url: https://www.destatis.de/static/DE/zensus/gitterdaten/Zensus2022_Geburtsland_Gruppen_in_Gitterzellen.zip

                - name: Staatsangehoerigkeit
                  status: not-active
                  table_name: staatsangehoerigkeit
                  year: 2022
                  url: https://www.destatis.de/static/DE/zensus/gitterdaten/Zensus2022_Staatsangehoerigkeit_in_Gitterzellen.zip

                - name: Staatsangehoerigkeit Gruppen
                  status: not-active
                  table_name: staatsangehoerigkeit_gruppen
                  year: 2022
                  url: https://www.destatis.de/static/DE/zensus/gitterdaten/Zensus2022_Staatsangehoerigkeit_Gruppen_in_Gitterzellen.zip

                - name: Zahl der Staatsangehoerigkeiten
                  status: not-active
                  table_name: staatsangehoerigkeiten_zahl
                  year: 2022
                  url: https://www.destatis.de/static/DE/zensus/gitterdaten/Zahl_der_Staatsangehoerigkeiten.zip

                - name: Durchschnittsalter
                  status: not-active
                  table_name: durchschnittsalter
                  year: 2022
                  url: https://www.destatis.de/static/DE/zensus/gitterdaten/Durchschnittsalter_in_Gitterzellen.zip

                - name: Altersgruppen (5 Klassen)
                  status: not-active
                  table_name: altersgruppen_5klassen
                  year: 2022
                  url: https://www.destatis.de/static/DE/zensus/gitterdaten/Alter_in_5_Altersklassen.zip

                - name: Altersgruppen (10 Jahre)
                  status: not-active
                  table_name: altersgruppen_10jahre
                  year: 2022
                  url: https://www.destatis.de/static/DE/zensus/gitterdaten/Alter_in_10er-Jahresgruppen.zip

                - name: Anteil unter 18
                  status: not-active
                  table_name: anteil_unter18
                  year: 2022
                  url: https://www.destatis.de/static/DE/zensus/gitterdaten/Anteil_unter_18-jaehrige_in_Gitterzellen.zip

                - name: Anteil ab 65
                  status: not-active
                  table_name: anteil_ab65
                  year: 2022
                  url: https://www.destatis.de/static/DE/zensus/gitterdaten/Anteil_ab_65-jaehrige_in_Gitterzellen.zip

                - name: Altersgruppen Infrastruktur
                  status: not-active
                  table_name: altersgruppen_infrastruktur
                  year: 2022
                  url: https://www.destatis.de/static/DE/zensus/gitterdaten/Alter_in_infrastrukturellen_Altersgruppen.zip

                - name: Familienstand
                  status: not-active
                  table_name: familienstand
                  year: 2022
                  url: https://www.destatis.de/static/DE/zensus/gitterdaten/Familienstand_in_Gitterzellen.zip

                - name: Religion
                  status: not-active
                  table_name: religion
                  year: 2022
                  url: https://www.destatis.de/static/DE/zensus/gitterdaten/Religion.zip

                - name: Durchschnittliche Haushaltsgroesse
                  status: active
                  table_name: durchschn_haushaltsgroesse
                  year: 2022
                  url: https://www.destatis.de/static/DE/zensus/gitterdaten/Durchschnittliche_Haushaltsgroesse_in_Gitterzellen.zip

                - name: Haushaltsgroesse
                  status: not-active
                  table_name: haushaltsgroesse
                  year: 2022
                  url: https://www.destatis.de/static/DE/zensus/gitterdaten/Zensus2022_Groesse_des_privaten_Haushalts_in_Gitterzellen.zip

                - name: Kernfamilie nach Kindern
                  status: not-active
                  table_name: kernfamilie_kinder
                  year: 2022
                  url: https://www.destatis.de/static/DE/zensus/gitterdaten/Typ_der_Kernfamilie_nach_Kindern.zip

                - name: Kernfamilie Groesse
                  status: not-active
                  table_name: kernfamilie_groesse
                  year: 2022
                  url: https://www.destatis.de/static/DE/zensus/gitterdaten/Groesse_der_Kernfamilie.zip

                - name: Privathaushalt Lebensform
                  status: not-active
                  table_name: privathaushalt_lebensform
                  year: 2022
                  url: https://www.destatis.de/static/DE/zensus/gitterdaten/Typ_des_privaren_Haushalts_Lebensform.zip

                - name: Privathaushalt Familie
                  status: not-active
                  table_name: privathaushalt_familien
                  year: 2022
                  url: https://www.destatis.de/static/DE/zensus/gitterdaten/Typ_des_privaten_Haushalts_Familien.zip

                - name: Seniorenstatus im Privathaushalt
                  status: not-active
                  table_name: seniorenstatus
                  year: 2022
                  url: https://www.destatis.de/static/DE/zensus/gitterdaten/Seniorenstatus_eines_privaten_Haushalts.zip

                - name: Nettokaltmiete
                  status: not-active
                  table_name: nettokaltmiete
                  year: 2022
                  url: https://www.destatis.de/static/DE/zensus/gitterdaten/Zensus2022_Durchschn_Nettokaltmiete.zip

                - name: Nettokaltmiete + Anzahl Wohnungen
                  status: not-active
                  table_name: nettokaltmiete_anzahl_wohnungen
                  year: 2022
                  url: https://www.destatis.de/static/DE/zensus/gitterdaten/Durchschnittliche_Nettokaltmiete_und_Anzahl_der_Wohnungen.zip

                - name: Eigentuemerquote
                  status: active
                  table_name: eigentuemerquote
                  year: 2022
                  url: https://www.destatis.de/static/DE/zensus/gitterdaten/Eigentuemerquote_in_Gitterzellen.zip

                - name: Leerstandsquote
                  status: not-active
                  table_name: leerstandsquote
                  year: 2022
                  url: https://www.destatis.de/static/DE/zensus/gitterdaten/Leerstandsquote_in_Gitterzellen.zip

                - name: Marktaktive Leerstandsquote
                  status: not-active
                  table_name: leerstandsquote_marktaktiv
                  year: 2022
                  url: https://www.destatis.de/static/DE/zensus/gitterdaten/Marktaktive_Leerstandsquote_in_Gitterzellen.zip

                - name: Wohnflaeche je Bewohner
                  status: not-active
                  table_name: wohnflaeche_bewohner
                  year: 2022
                  url: https://www.destatis.de/static/DE/zensus/gitterdaten/Durchschnittliche_Wohnflaeche_je_Bewohner_in_Gitterzellen.zip

                - name: Flaeche je Wohnung
                  status: active
                  table_name: wohnflaeche_wohnung
                  year: 2022
                  url: https://www.destatis.de/static/DE/zensus/gitterdaten/Durchschnittliche_Flaeche_je_Wohnung_in_Gitterzellen.zip

                - name: Flaeche Wohnung 10m2 Intervalle
                  status: not-active
                  table_name: wohnflaeche_wohnung_intervalle
                  year: 2022
                  url: https://www.destatis.de/static/DE/zensus/gitterdaten/Flaeche_der_Wohnung_10m2_Intervalle.zip

                - name: Wohnungen nach Gebaeudetyp und Groesse
                  status: not-active
                  table_name: wohnungen_gebaeudetyp
                  year: 2022
                  url: https://www.destatis.de/static/DE/zensus/gitterdaten/Wohnungen_nach_Gebaeudetyp_Groesse.zip

                - name: Wohnungen nach Raeumen
                  status: not-active
                  table_name: wohnungen_raeume
                  year: 2022
                  url: https://www.destatis.de/static/DE/zensus/gitterdaten/Wohnungen_nach_Zahl_der_Raeume.zip

                - name: Gebaeude nach Baujahr (Jahrzehnte)
                  status: not-active
                  table_name: gebaeude_baujahr_jahrzehnte
                  year: 2022
                  url: https://www.destatis.de/static/DE/zensus/gitterdaten/Gebaeude_nach_Baujahr_Jahrzehnte.zip

                - name: Gebaeude nach Baujahr (Mikrozensus)
                  status: active
                  table_name: gebaeude_baujahr_mikrozensus
                  year: 2022
                  url: https://www.destatis.de/static/DE/zensus/gitterdaten/Gebaeude_nach_Baujahr_in_Mikrozensus_Klassen.zip

                - name: Gebaeude nach Anzahl Wohnungen
                  status: active
                  table_name: gebaeude_anzahl_wohnungen
                  year: 2022
                  url: https://www.destatis.de/static/DE/zensus/gitterdaten/Gebaeude_nach_Anzahl_der_Wohnungen_im_Gebaeude.zip

                - name: Gebaeude nach Typ und Groesse
                  status: active
                  table_name: gebaeude_typ_groesse
                  year: 2022
                  url: https://www.destatis.de/static/DE/zensus/gitterdaten/Gebaeude_mit_Wohnraum_nach_Gebaeudetyp_Groesse.zip

                - name: Heizungsart
                  status: active
                  table_name: heizungsart
                  year: 2022
                  url: https://www.destatis.de/static/DE/zensus/gitterdaten/Zensus2022_Heizungsart.zip

                - name: Heizungsart (ueberwiegend)
                  status: active
                  table_name: heizungsart_ueberwiegend
                  year: 2022
                  url: https://www.destatis.de/static/DE/zensus/gitterdaten/Gebaeude_mit_Wohnraum_nach_ueberwiegender_Heizungsart.zip

                - name: Energietraeger
                  status: active
                  table_name: energietraeger
                  year: 2022
                  url: https://www.destatis.de/static/DE/zensus/gitterdaten/Zensus2022_Energietraeger.zip

                - name: Energietraeger der Heizung
                  status: active
                  table_name: energietraeger_heizung
                  year: 2022
                  url: https://www.destatis.de/static/DE/zensus/gitterdaten/Gebaeude_mit_Wohnraum_nach_Energietraeger_der_Heizung.zip

                - name: Auslaenderanteil EU/nichtEU
                  status: not-active
                  table_name: auslaenderanteil_eu_nichteu
                  year: 2022
                  url: https://www.destatis.de/static/DE/zensus/gitterdaten/Auslaenderanteil_EU_nichtEU_Gitterzellen.zip

                - name: Gebaeude nach Baujahresklassen
                  status: not-active
                  table_name: gebaeude_baujahresklassen
                  year: 2022
                  url: https://www.destatis.de/static/DE/zensus/gitterdaten/Gebaeude_nach_Baujahresklassen_in_Gitterzellen.zip

                - name: Shapefile Zensus 2022
                  status: not-active
                  table_name: shapefile_2022
                  year: 2022
                  url: https://www.destatis.de/static/DE/zensus/gitterdaten/Shapefile_Zensus2022.zip

                - name: Bevoelkerung (100m Raster)
                  status: not-active
                  table_name: bevoelkerung_100m
                  year: 2011
                  url: http://www.destatis.de/static/DE/zensus/2011_gitterdaten/Download-Tabelle_Bevoelkerung_im_100_Meter-Gitter.zip

                - name: Demographie (100m Raster)
                  status: not-active
                  table_name: demographie_100m
                  year: 2011
                  url: http://www.destatis.de/static/DE/zensus/2011_gitterdaten/Demographie_100_Meter-Gitter.zip

                - name: Familien (100m Raster)
                  status: not-active
                  table_name: familien_100m
                  year: 2011
                  url: http://www.destatis.de/static/DE/zensus/2011_gitterdaten/Download-Tabelle_Familien_im_100_Meter-Gitter.zip

                - name: Haushalte (100m Raster)
                  status: not-active
                  table_name: haushalte_100m
                  year: 2011
                  url: http://www.destatis.de/static/DE/zensus/2011_gitterdaten/Tabelle_Haushalt_im_100_Meter-Gitter.zip

                - name: Wohnungen (100m Raster)
                  status: not-active
                  table_name: wohnungen_100m
                  year: 2011
                  url: http://www.destatis.de/static/DE/zensus/2011_gitterdaten/Wohnungen_im_100_Meter-Gitter.zip

                - name: Gebaeude und Wohnungen (100m Raster)
                  status: not-active
                  table_name: gebaeude_wohnungen_100m
                  year: 2011
                  url: http://www.destatis.de/static/DE/zensus/2011_gitterdaten/Gebaeude_und_Wohnungen_im_100_Meter-Gitter.zip

                - name: Klassierte Werte (1km Raster)
                  status: not-active
                  table_name: klassierte_werte_1km
                  year: 2011
                  url: http://www.destatis.de/static/DE/zensus/2011_gitterdaten/Tabelle_und_Datensatzbeschreibung_Klassierte_Werte_im_ein_Kilometer-Gitter.zip

                - name: Spitze Werte (1km Raster)
                  status: not-active
                  table_name: spitze_werte_1km
                  year: 2011
                  url: http://www.destatis.de/static/DE/zensus/2011_gitterdaten/Tabelle_und_Datensatzbeschreibung_Spitze_Werte_im_ein_Kilometer-Gitter.zip

                - name: Shapefile 1km Raster
                  status: not-active
                  table_name: shapefile_1km
                  year: 2011
                  url: http://www.destatis.de/static/DE/zensus/2011_gitterdaten/Shapefile_eines_ein_Kilometer-Gitters_fuer_Deutschland_INSPIRE_konform.zip

                - name: Shapefile Zensus 2011
                  status: not-active
                  table_name: shapefile_2011
                  year: 2011
                  url: https://www.destatis.de/static/DE/zensus/2011_gitterdaten/Shapefile_Zensus2011.zip
        openmeteo:
          status: active
          schema: opendata
          prefix: openmeteo
          path:
              base: "{infdb-loader/path/opendata}/openmeteo/"
              processed: "{infdb-loader/path/processed}/openmeteo/"
          timing:
              start_time: "2020-01-01"
              end_time: "2024-12-31"
              temporal: hourly  # options: hourly, daily
          data:
              - temperature_2m
              - wind_speed_10m
              - precipitation
        kwp-nrw:
          status: not-active
          schema: opendata
          prefix: kwp_nrw
          path:
              base: "{infdb-loader/path/opendata}/kwp-nrw/"
              zip: "{infdb-loader/sources/kwp-nrw/path/base}/zip/"
              unzip: "{infdb-loader/sources/kwp-nrw/path/base}/unzip/"
              processed: "{infdb-loader/path/processed}/kwp-nrw/"
          datasets:
              - name: Waermebedarf
                status: active
                table_name: waermebedarf
                url: https://www.opengeodata.nrw.de/produkte/umwelt_klima/energie/kwp/KWP-NRW-Waermebedarf_EPSG25832_Geodatabase.zip
                layer:
                  - "Waermelinien"
                  - "Raumwaermebedarf_ist"
                  - "Gebaeude_Fortschreibung_moderat"
                  - "Gebaeude_Fortschreibung_hoch"
                  - "Gebaeude_Fortschreibung_erhoeht"
              - name: Energietraeger
                status: active
                table_name: energietraeger
                url: https://www.opengeodata.nrw.de/produkte/umwelt_klima/energie/kwp/KWP-NRW-Energietraeger-Sanierung-Baubloecke-Flure-NRW_EPSG25832_Geodatabase.zip
                layer:
                  - "Flur_Sanierung_Energietraeger_OpenData"
                  - "Baublock_Sanierung_Energietraeger_OpenData"
              - name: Waermelinien
                status: active
                table_name: waermelinien
                url: https://www.opengeodata.nrw.de/produkte/umwelt_klima/energie/kwp/KWP-NRW-Waermelinien_EPSG25832_Geodatabase.zip
                layer:
                  - "Waermelinien"
              - name: Tiefe Geothermie
                status: active
                table_name: geothermie_tief
                url: https://www.opengeodata.nrw.de/produkte/umwelt_klima/energie/kwp/KWP-NRW-Potenzial_TG_Raster_EPSG25832_Geodatabase.zip
                layer:
                  - "MTG_TG_Raster_NRW"
              - name: Oberflaechennahe Geothermie
                status: active
                table_name: geothermie_oberflaeche
                url: https://www.opengeodata.nrw.de/produkte/umwelt_klima/energie/kwp/KWP-NRW-Potenzial_ONG_MTG_Baublock_EPSG25832_Geodatabase.zip
                layer:
                  - "Pot_ONG_MTG_NRW"
              - name: Freiflaechen Solarthermie
                status: active
                table_name: solarthermie_freiflaeche
                url: https://www.opengeodata.nrw.de/produkte/umwelt_klima/energie/kwp/KWP-NRW-Potenzial_FF_Solarthermie_Flur_EPSG25832_Geodatabase.zip
                layer:
                  - "FF_Solarthermie_Flur_NRW"
        gebaeude-neuburg:
            status: active
            url: "https://cloud.ocd.need.energy/remote.php/dav/spaces/cd8ca458-980c-41e6-9cd2-5137f78e039d$e57dd96e-a735-4f09-99df-dd4f1a992a20/gebaeudedaten-neuburg.gpkg"
            protocol: webdav
            username: "need"

            path:
                base: "{infdb-loader/path/opendata}/gebaeude-neuburg/"
                processed: "{infdb-loader/path/processed}/gebaeude-neuburg/"
            schema: need_intern
            layer:
                - "gebaeudedaten_neuburg"
        waermeatlas-hessen-bensheim:
            status: active
            url: "https://cloud.ocd.need.energy/remote.php/dav/spaces/cd8ca458-980c-41e6-9cd2-5137f78e039d$e57dd96e-a735-4f09-99df-dd4f1a992a20/BensheimWaermeatlasHessen.gpkg"
            protocol: webdav
            username: "need"
            path:
                base: "{infdb-loader/path/opendata}/waermeatlas-hessen-bensheim/"
                processed: "{infdb-loader/path/processed}/waermeatlas-hessen-bensheim/"
            schema: need_intern
            prefix: "waermeatlas_hessen_bensheim"
            layer:
                - "WAH_Punkte"
                - "WAH_Strassenabschnitte"
                - "WAH_Raster_1km"
                - "WAH_Raster_100m"
                - "WAH_Baubloecke"
                - "Hausumringe"
                - "Flurstuecke"
        tudo-basemap-ways:
            status: active
            url: "https://cloud.ocd.need.energy/remote.php/dav/spaces/cd8ca458-980c-41e6-9cd2-5137f78e039d$e57dd96e-a735-4f09-99df-dd4f1a992a20/TUDO_Basemap_ways.geojson"
            protocol: webdav
            username: "need"
            path:
                base: "{infdb-loader/path/opendata}/tudo-basemap-ways/"
                processed: "{infdb-loader/path/processed}/tudo-basemap-ways/"
            schema: need_intern
            layer: 
                - "Basemap_Oberhausen_Neuburg_Sonthofen_Verkehrslinie"
```

1. Name of the project / scenario. This is used for folder naming in the `processed` directory.
2. List of municipality keys (AGS - Amtlicher Gemeindeschlüssel) that define the geographical scope for which data will be imported.
3. Path to the centralized database configuration file.
4. Database connection parameters. If set to `None`, the loader will use the values defined in the main configuration file referenced in point (3).
5. Toggles the specific data source package on (`active`) or off (`not-active`).



## Jupyter Notebook
Jupyter Notebook allows you to interactively work with the data by running Python scripts and visualize results directly within your browser. It is an excellent tool for data exploration and prototyping. More information can be found on the official [Website](https://jupyter.org/).

The configuration 
``` bash title=".env"
# ==============================================================================
# SERVICE ACTIVATION
# ==============================================================================
# Select profiles to activate
COMPOSE_PROFILES=...,notebook,...  # (1)

# ==============================================================================
# JUPYTER NOTEBOOK (Development Environment)
# ==============================================================================
# Profile: notebook

# Port to expose Jupyter on the host machine
SERVICES_JUPYTER_EXPOSED_PORT=8888 # (2)

# Enable Jupyter Lab interface (yes/no)
SERVICES_JUPYTER_ENABLE_LAB=yes

# Authentication token for Jupyter
SERVICES_JUPYTER_TOKEN=infdb # (3)

# Path to notebook files
SERVICES_JUPYTER_PATH_BASE=..src/notebooks/
```

1. Profile "notebook" must be within the list to activate Jupyter service
2. Port on which the Jupyter is available 
3. Token for identification to Jupyter Notebook

If you activate the service, it should be available on the default port `SERVICES_JUPYTER_EXPOSED_PORT=8888` via your browser:
=== "Local"
    http://localhost/8888

=== "Remote"
    http://IP-ADDRESS-OF-HOST/8888


## QGIS Webclient (QWC)
The **QGIS Web Client 2 (QWC2)** service provides a modern, responsive web application for visualizing and interacting with QGIS projects. It allows users to publish geospatial data stored in the infDB database to the web, offering capabilities such as layer management, feature information, searching, and map printing. More information can be found on the official [Github Repo](https://github.com/qgis/qwc2/).

The configuration:
``` bash title=".env"
# ==============================================================================
# SERVICE ACTIVATION
# ==============================================================================
# Select profiles to activate
COMPOSE_PROFILES=...,qwc,...  # (1)

# ==============================================================================
# QGIS WEB CLIENT (QWC)
# ==============================================================================
# Profile: qwc

# Port for QWC web interface
SERVICES_QWC_EXPOSED_PORT_GUI=80 # (2)

# Port for QWC internal database
SERVICES_QWC_EXPOSED_PORT_DB=5434

# Password for QWC PostgreSQL database
SERVICES_QWC_POSTGRES_PASSWORD=infdb

# JWT secret key for QWC (change this for production!)
JWT_SECRET_KEY=change-me-in-production
```

1. Profile "qwc" must be within the list to activate the QGIS Web Client service
2. Port on which the Web GIS interface is available

If you activate the service, it should be available on the default port `SERVICES_QWC_EXPOSED_PORT=80` via your browser:
=== "Local"
    http://localhost:80

=== "Remote"
    http://IP-ADDRESS-OF-HOST/80

## pgAdmin
pgAdmin is a comprehensive administration and management tool for the PostgreSQL database. It provides a graphical interface to simplify the creation, maintenance, and use of database objects, including executing SQL queries and managing users. More information can be found on the official [Website](https://www.pgadmin.org/).

The configuration:
``` bash title=".env"
# ==============================================================================
# SERVICE ACTIVATION
# ==============================================================================
# Select profiles to activate
COMPOSE_PROFILES=...,admin,...  # (1)

# ==============================================================================
# PGADMIN (Database Administration Interface)
# ==============================================================================
# Profile: admin

# Default login credentials for pgAdmin
SERVICES_PGADMIN_DEFAULT_EMAIL=admin@need.energy # (3)
SERVICES_PGADMIN_DEFAULT_PASSWORD=infdb # (4)

# Port to expose pgAdmin on the host machine
SERVICES_PGADMIN_EXPOSED_PORT=82 # (2)
```

1. Profile "admin" must be within the list to activate the pgAdmin service
2. Port on which the pgAdmin interface is available (Default: 82)
3. Default email used to log in to the pgAdmin interface
4. Default password used to log in to the pgAdmin interface

If you activate the service, it should be available on the default port `SERVICES_PGADMIN_EXPOSED_PORT=82` via your browser:
=== "Local"
    http://localhost:82

=== "Remote"
    http://IP-ADDRESS-OF-HOST/82


## API
The API service provides programmable access to the infDB platform functionality through RESTful endpoints. This facilitates integration with external applications, automated data retrieval, and custom service orchestration.

The configuration:
``` bash title=".env"
# ==============================================================================
# SERVICE ACTIVATION
# ==============================================================================
# Select profiles to activate
COMPOSE_PROFILES=...,api,...  # (1)

# ==============================================================================
# FASTAPI (REST API Service)
# ==============================================================================
# Profile: api

# Port for the FastAPI service
SERVICES_API_PORT=8000


# ==============================================================================
# PYGEOAPI (OGC API Service)
# ==============================================================================
# Profile: api

# Port for the PyGeoAPI service
SERVICES_PYGEOAPI_PORT=8001

# Host IP to run PyGeoAPI on (e.g., localhost or 10.162.28.144)
SERVICES_PYGEOAPI_BASE_HOST=localhost


# ==============================================================================
# POSTGREST (PostgreSQL REST API)
# ==============================================================================
# Profile: api

# Port for the PostgREST service
SERVICES_POSTGREST_PORT=8002
```

1. Profile "api" must be within the list to activate the API service
2. Port on which the API endpoints are available

If you activate the service, the documentation (Swagger UI) should be available on the default port `SERVICES_API_EXPOSED_PORT=8000` via your browser:
=== "Local"
    http://localhost:8000/docs

=== "Remote"
    http://IP-ADDRESS-OF-HOST/8000/docs



## OpenCloud :material-cloud:
OpenCloud is a self-hosted file share and communication platform. It allows users to store files, contacts, calendars, and more on the server, providing an easy way to manage data needed for import or export processes within the infDB ecosystem. More information can be found on the official [Website](https://opencloud.eu/).

The OpenCloud service need to be started manually. A comprehensive Readme can be found in `services/opencloud/Readme.md`
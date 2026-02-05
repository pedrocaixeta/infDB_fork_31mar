DROP TABLE IF EXISTS citydb.surface_prefix;
CREATE TABLE IF NOT EXISTS citydb.surface_prefix AS
    WITH gemeindeschluessel_data AS (SELECT feature_id, val_string
                           FROM citydb.property
                           WHERE name = 'Gemeindeschluessel')
    SELECT
        id AS surface_id,
        objectid AS surface_objectid,
        split_part(objectid, '_', 1) || '_' ||
        split_part(objectid, '_', 2) || '_' ||
        split_part(objectid, '_', 3) AS building_objectid,
        objectclass_id
        -- gsd.val_string as gemeindeschluessel,
        -- substring(gsd.val_string, 1, 2) as ags_id
    FROM citydb.feature
    WHERE objectclass_id IN (709, 710, 712);
    CREATE INDEX IF NOT EXISTS idx_surface_prefix_building_objectid
        ON citydb.surface_prefix (building_objectid);
    CREATE INDEX IF NOT EXISTS idx_surface_prefix_surface_id
        ON citydb.surface_prefix (surface_id);
    -- CREATE INDEX IF NOT EXISTS idx_surface_prefix_ags_id
    --     ON citydb.surface_prefix (ags_id);
    -- CREATE INDEX IF NOT EXISTS idx_surface_prefix_gemeindeschluessel
    --     ON citydb.surface_prefix (gemeindeschluessel);


-- DROP TABLE IF EXISTS {output_schema}.{table_name};
CREATE TABLE IF NOT EXISTS {output_schema}.{table_name} AS
    SELECT
        sp.surface_objectid AS surface_gmlid,
        sp.objectclass_id,
        -- sp.gemeindeschluessel,
        CASE sp.objectclass_id
            WHEN 709 THEN 'WallSurface'
            WHEN 710 THEN 'GroundSurface'
            WHEN 712 THEN 'RoofSurface'
            ELSE 'Other'
            END AS surface_type,

        sp.building_objectid,

        ST_Multi(
                ST_CollectionExtract(gd.geometry, 3)
        )::geometry(MultiPolygonZ, 25832) AS geometry,

        MAX(
                CASE
                    WHEN p.name = 'Flaeche'
                        THEN p.val_string::double precision
                    END
        ) AS area,

        MAX(
                CASE
                    WHEN p.name = 'Z_MIN'
                        THEN p.val_string::double precision
                    END
        ) AS z_min,

        MAX(
                CASE
                    WHEN p.name = 'Z_MIN_ASL'
                        THEN p.val_string::double precision
                    END
        ) AS z_min_asl,

        MAX(
                CASE
                    WHEN p.name = 'Z_MAX'
                        THEN p.val_string::double precision
                    END
        ) AS z_max,

        MAX(
                CASE
                    WHEN p.name = 'Z_MAX_ASL'
                        THEN p.val_string::double precision
                    END
        ) AS z_max_asl

    FROM citydb.surface_prefix sp
             JOIN citydb.geometry_data gd
                  ON gd.feature_id = sp.surface_id
             LEFT JOIN citydb.property p
                       ON p.feature_id = sp.surface_id
    -- WHERE sp.gemeindeschluessel IN ({gemeindeschluessel})
    
    GROUP BY
        sp.surface_objectid,
        sp.objectclass_id,
        -- sp.gemeindeschluessel,
        sp.building_objectid,
        gd.geometry;
CREATE INDEX IF NOT EXISTS idx_buildings_surfaces_building_objectid
    ON {output_schema}.{table_name} (building_objectid);
CREATE INDEX IF NOT EXISTS idx_buildings_surfaces_surface_type
    ON {output_schema}.{table_name} (surface_type);
CREATE INDEX IF NOT EXISTS idx_buildings_surfaces_geom
    ON {output_schema}.{table_name} USING GIST (geometry);
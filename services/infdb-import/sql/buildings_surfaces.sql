DROP TABLE IF EXISTS citydb.surface_prefix;
CREATE TABLE citydb.surface_prefix AS
    SELECT
        id AS surface_id,
        objectid AS surface_objectid,
        split_part(objectid, '_', 1) || '_' ||
        split_part(objectid, '_', 2) || '_' ||
        split_part(objectid, '_', 3) AS building_objectid,
        objectclass_id
    FROM feature
    WHERE objectclass_id IN (709, 710, 712);
CREATE INDEX idx_surface_prefix_building_objectid
    ON citydb.surface_prefix (building_objectid);
CREATE INDEX idx_surface_prefix_surface_id
    ON citydb.surface_prefix (surface_id);


DROP TABLE IF EXISTS opendata.buildings_surfaces;
CREATE TABLE opendata.buildings_surfaces AS
    SELECT
        sp.surface_objectid AS surface_gmlid,
        sp.objectclass_id,

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

    FROM surface_prefix sp
             JOIN geometry_data gd
                  ON gd.feature_id = sp.surface_id
             LEFT JOIN property p
                       ON p.feature_id = sp.surface_id

    GROUP BY
        sp.surface_objectid,
        sp.objectclass_id,
        sp.building_objectid,
        gd.geometry;
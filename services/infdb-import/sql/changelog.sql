DROP TABLE IF EXISTS public.changelog CASCADE;
CREATE TABLE IF NOT EXISTS  public.changelog (
    id          BIGSERIAL PRIMARY KEY,
    tool        TEXT,
    modified_at TIMESTAMPTZ DEFAULT NOW(),
    comment     TEXT,
    modified_by TEXT
);

CREATE OR REPLACE FUNCTION public.fn_begin_changelog(p_tool TEXT, p_comment TEXT DEFAULT NULL, p_modified_by TEXT DEFAULT NULL)
RETURNS BIGINT AS $$
DECLARE
    v_changelog_id BIGINT;
BEGIN
    INSERT INTO public.changelog (tool, modified_at, comment, modified_by)
    VALUES (p_tool, NOW(), p_comment, p_modified_by)
    RETURNING id INTO v_changelog_id;

    RETURN v_changelog_id;
END;
$$ LANGUAGE plpgsql;
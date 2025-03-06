-- TRIGGER 1: EVENTS
CREATE TRIGGER trigger_update_event_genre_distribution AFTER INSERT OR DELETE OR UPDATE ON public.event_genres 
FOR EACH ROW EXECUTE FUNCTION update_event_genre_distribution()

-- FUNCTION 1
CREATE OR REPLACE FUNCTION public.update_event_genre_distribution()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
DECLARE
    genre_json JSONB;
BEGIN
    -- Initialize genre distribution with all genres set to 0
    genre_json := jsonb_build_object(
        'house', 0,
        'edm', 0,
        'reggaeton', 0,
        'd&b', 0,
        'techno', 0,
        'deep house', 0,
        'afro house', 0
    );

    -- Update genre distribution based on existing event_genres
    SELECT jsonb_object_agg(g.name, 1) 
    INTO genre_json
    FROM event_genres eg
    JOIN genres g ON eg.genre_id = g.id
    WHERE eg.event_id = NEW.event_id;

    -- Ensure all genres are present in genre_json (set missing ones to 0)
    genre_json := genre_json || jsonb_build_object(
        'house', COALESCE(genre_json->>'house', '0')::INT,
        'edm', COALESCE(genre_json->>'edm', '0')::INT,
        'reggaeton', COALESCE(genre_json->>'reggaeton', '0')::INT,
        'd&b', COALESCE(genre_json->>'d&b', '0')::INT,
        'techno', COALESCE(genre_json->>'techno', '0')::INT,
        'deep house', COALESCE(genre_json->>'deep house', '0')::INT,
        'afro house', COALESCE(genre_json->>'afro house', '0')::INT
    );

    -- Update event_data with the new genre_dist value
    UPDATE event_data
    SET genre_dist = genre_json
    WHERE id = NEW.event_id;

    RETURN NEW;
END;
$function$

-- TRIGGER 2: DJ
CREATE TRIGGER trigger_update_dj_genre_distribution AFTER INSERT OR DELETE OR UPDATE ON public.dj_genres 
FOR EACH ROW EXECUTE FUNCTION update_dj_genre_distribution()

-- FUNCTION 2:
CREATE OR REPLACE FUNCTION public.update_dj_genre_distribution()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
DECLARE
    genre_json JSONB;
BEGIN
    -- Initialize genre distribution with all genres set to 0
    genre_json := jsonb_build_object(
        'house', 0,
        'edm', 0,
        'reggaeton', 0,
        'd&b', 0,
        'techno', 0,
        'deep house', 0,
        'afro house', 0
    );

    -- Update genre distribution based on existing event_genres
    SELECT jsonb_object_agg(g.name, 1) 
    INTO genre_json
    FROM dj_genres dg
    JOIN genres g ON dg.genre_id = g.id
    WHERE dg.dj_id = NEW.dj_id;

    -- Ensure all genres are present in genre_json (set missing ones to 0)
    genre_json := genre_json || jsonb_build_object(
        'house', COALESCE(genre_json->>'house', '0')::INT,
        'edm', COALESCE(genre_json->>'edm', '0')::INT,
        'reggaeton', COALESCE(genre_json->>'reggaeton', '0')::INT,
        'd&b', COALESCE(genre_json->>'d&b', '0')::INT,
        'techno', COALESCE(genre_json->>'techno', '0')::INT,
        'deep house', COALESCE(genre_json->>'deep house', '0')::INT,
        'afro house', COALESCE(genre_json->>'afro house', '0')::INT
    );

    -- Update dj with the new genre_dist value
    UPDATE dj
    SET genre_dist = genre_json
    WHERE id = NEW.dj_id;

    RETURN NEW;
END;
$function$
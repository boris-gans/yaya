-- UPDATE VENUE STATS: completed count, avg age, gender ratio
CREATE OR REPLACE FUNCTION public.update_venue_stats()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
DECLARE
    -- Averages for gender ratio and age
    avg_gender_ratio FLOAT;
    avg_age_val FLOAT;
    
    -- For averaging language distribution
    event_count INT;
    target_venue_id INT;
BEGIN
	-- 	Establish venue_id var
	SELECT venue_id 
    INTO target_venue_id
    FROM event_data
    WHERE id = NEW.event_id;

    -- Calculate average gender ratio across completed events
    SELECT AVG(e.gender_ratio)
    INTO avg_gender_ratio
    FROM event_data e
    JOIN published_events pe ON e.id = pe.event_id
    WHERE e.venue_id = (
    	SELECT venue_id
    	FROM event_data
    	WHERE id = NEW.event_id
    )
    AND pe.completed = TRUE;

    -- Calculate average age across completed events
    SELECT AVG(e.avg_age)
    INTO avg_age_val
    FROM event_data e
    JOIN published_events pe ON e.id = pe.event_id
    WHERE e.venue_id = (
    	SELECT venue_id
    	FROM event_data
    	WHERE id = NEW.event_id
    ) 
    AND pe.completed = TRUE;

      -- Count completed events for this DJ
    SELECT COUNT(*)
    INTO event_count
    FROM published_events pe
    JOIN event_dj ed ON pe.event_id = ed.event_id
    WHERE ed.dj_id = target_dj_id
    AND pe.completed = TRUE;


    -- Update the venues table with the calculated averages
    UPDATE venues
    SET 
        gender_ratio = avg_gender_ratio,
        avg_age = avg_age_val,
        completed_events_count = event_count
    WHERE id = target_venue_id;

    RETURN NEW;
END;
$function$

-- UPDATE DJ STATS: completed events count, avg age, gender ratio
CREATE OR REPLACE FUNCTION public.update_dj_stats()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
DECLARE
    -- Averages for gender ratio and age
    avg_gender_ratio FLOAT;
    avg_age_val FLOAT;
    
    -- For averaging language distribution
    event_count INT;
    target_dj_id INT;
BEGIN
    -- Loop through all DJs for the current event
    FOR target_dj_id IN 
        SELECT dj_id
        FROM event_dj
        WHERE event_id = NEW.event_id
    LOOP
        -- Calculate average gender ratio across completed events for the DJ
        SELECT AVG(e.gender_ratio)
        INTO avg_gender_ratio
        FROM event_data e
        JOIN published_events pe ON e.id = pe.event_id
        JOIN event_dj ed ON e.id = ed.event_id
        WHERE ed.dj_id = target_dj_id
        AND pe.completed = TRUE;

        -- Calculate average age across completed events for the DJ
        SELECT AVG(e.avg_age)
        INTO avg_age_val
        FROM event_data e
        JOIN published_events pe ON e.id = pe.event_id
        JOIN event_dj ed ON e.id = ed.event_id
        WHERE ed.dj_id = target_dj_id
        AND pe.completed = TRUE;

        -- Count completed events for this DJ
        SELECT COUNT(*)
        INTO event_count
        FROM published_events pe
        JOIN event_dj ed ON pe.event_id = ed.event_id
        WHERE ed.dj_id = target_dj_id
        AND pe.completed = TRUE;

        -- Update the DJ table with the calculated averages
        UPDATE dj
        SET 
            gender_ratio = avg_gender_ratio,
            avg_age = avg_age_val,
            completed_events_count = event_count
        WHERE id = target_dj_id;
    END LOOP;

    RETURN NEW;
END;
$function$

-- TRIGEGR 1
CREATE TRIGGER check_event_dj_reference_trigger BEFORE INSERT ON public.published_events 
FOR EACH ROW EXECUTE FUNCTION check_event_dj_reference()

-- FUNCTION 1
CREATE OR REPLACE FUNCTION public.check_event_dj_reference()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
    -- Check if the event_id exists in event_dj before allowing insert in published_events
    IF NOT EXISTS (SELECT 1 FROM event_dj WHERE event_id = NEW.event_id) THEN
        RAISE EXCEPTION 'event_id % must be referenced in event_dj table before being inserted into published_events', NEW.event_id;
    END IF;

    RETURN NEW;
END;
$function$


-- TRIGGER 2
CREATE TRIGGER trigger_completed_event AFTER UPDATE OF completed ON public.published_events 
FOR EACH ROW WHEN ((new.completed = true)) EXECUTE FUNCTION insert_completed_event_metrics()

-- FUNCTION 2
CREATE OR REPLACE FUNCTION public.insert_completed_event_metrics()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
DECLARE
    total_tickets_sold INT;
    venue_capacity INT;
    num_clicks INT;
    num_impressions INT;
    table_purchases INT;
    table_count INT;
    total_ticket_revenue NUMERIC(7,2);
    calc_fill_ratio NUMERIC(4,3);
    ctr NUMERIC(4,3);
    conversion_rate NUMERIC(4,3);
    table_rate NUMERIC(4,3);
    avg_age NUMERIC(4,2);
    gender_ratio NUMERIC(3,2);
    english_count INT;
    spanish_count INT;
    dutch_count INT;
    english_ratio NUMERIC(3,2);
    spanish_ratio NUMERIC(3,2);
    dutch_ratio NUMERIC(3,2);
BEGIN
    -- Get event-related data
    SELECT 
        COUNT(*) INTO total_tickets_sold
    FROM purchase p
    WHERE p.event_id = NEW.event_id::INT;

	SELECT 
	    e.num_clicks, e.num_impressions, e.table_purchases, e.avg_age, e.gender_ratio, e.english_count, e.spanish_count, e.dutch_count
	    v.capacity, v.table_count
	INTO num_clicks, num_impressions, table_purchases, avg_age, gender_ratio, english_count, spanish_count, dutch_count, venue_capacity, table_count
	FROM event_data e
	JOIN venues v ON e.venue_id = v.id
	WHERE e.id = NEW.event_id;

    -- Get total ticket revenue
    SELECT 
        COALESCE(SUM(price), 0) INTO total_ticket_revenue
    FROM purchase
    WHERE event_id = NEW.event_id;

	RAISE NOTICE 'Pre Vals: % % %', venue_capacity, total_tickets_sold, num_impressions;

    -- Calculate metrics (handle division by zero)
    calc_fill_ratio := CASE WHEN venue_capacity > 0 THEN total_tickets_sold::NUMERIC / venue_capacity ELSE 0 END;
    ctr := CASE WHEN num_impressions > 0 THEN num_clicks::NUMERIC / num_impressions ELSE 0 END;
    conversion_rate := CASE WHEN num_impressions > 0 THEN total_tickets_sold::NUMERIC / num_impressions ELSE 0 END;
    table_rate := CASE WHEN table_count > 0 THEN table_purchases::NUMERIC / table_count ELSE 0 END;
    english_ratio := english_count::NUMERIC / total_tickets_sold END;
    spanish_ratio := spanish_count::NUMERIC / total_tickets_sold END;
    dutch_ratio := dutch_count::NUMERIC / total_tickets_sold END;

	RAISE NOTICE 'Vals: % % % %', calc_fill_ratio, ctr, conversion_rate, table_rate;

    -- Insert into completed_event_metrics
    INSERT INTO completed_event_metrics (
        event_id, completed_at, fill_ratio, ctr, conversion_rate, table_rate, ticket_revenue, avg_age, gender_ratio, english_ratio, spanish_ratio, dutch_ratio
    ) VALUES (
        NEW.event_id, CURRENT_TIMESTAMP, calc_fill_ratio, ctr, conversion_rate, table_rate, total_ticket_revenue, avg_age, gender_ratio, english_ratio, spanish_ratio, dutch_ratio
    );

    RETURN NEW;
END;
$function$

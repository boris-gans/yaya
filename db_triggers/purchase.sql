-- TRIGGER 1
CREATE TRIGGER trigger_update_event_stats AFTER INSERT OR DELETE ON public.purchase 
FOR EACH ROW EXECUTE FUNCTION update_event_data_purchase()

-- FUNCTION 1
CREATE OR REPLACE FUNCTION public.update_event_data_purchase()
 RETURNS trigger AS $$
DECLARE
    total_count INT;
    male_count INT;
    female_count INT;
    other_count INT;
    avg_age_val FLOAT;
    english_count_val INT;
    spanish_count_val INT;
    dutch_count_val INT;
BEGIN
    -- Calculate gender ratio
    SELECT 
        COUNT(*) FILTER (WHERE u.gender = 'Male') AS male_count,
        COUNT(*) FILTER (WHERE u.gender = 'Female') AS female_count,
        COUNT(*) FILTER (WHERE u.gender = 'Other') AS other_count,
        COUNT(*) FILTER (WHERE u.language = 'english') AS english_count_val,
        COUNT(*) FILTER (WHERE u.language = 'spanish') AS spanish_count_val,
        COUNT(*) FILTER (WHERE u.language = 'dutch') AS dutch_count_val,
        COUNT(*) AS total_count
    INTO male_count, female_count, other_count, english_count_val, spanish_count_val, dutch_count_val, total_count
    FROM purchase p
    JOIN user_data u ON p.user_id = u.id
    WHERE p.event_id = NEW.event_id;

	RAISE NOTICE 'Vals: % % %', english_count_val, spanish_count_val, dutch_count_val;

    -- Calculate gender ratio logic
    IF total_count > 0 THEN
        IF male_count >= female_count THEN
            male_count := male_count + other_count;
        ELSE
            female_count := female_count + other_count;
        END IF;
    END IF;
    
    -- Calculate average age
    SELECT AVG(EXTRACT(YEAR FROM AGE(u.birthdate))) 
    INTO avg_age_val
    FROM purchase p
    JOIN user_data u ON p.user_id = u.id
    WHERE p.event_id = NEW.event_id;
    
    -- Update event_data with the new values
    UPDATE event_data
    SET 
        gender_ratio = CASE 
                    WHEN total_count > 0 THEN female_count::FLOAT / total_count
                    ELSE 0 
                  END,
        avg_age = avg_age_val,
        english_count = english_count_val,
        spanish_count = spanish_count_val,
        dutch_count = dutch_count_val
    WHERE id = NEW.event_id;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
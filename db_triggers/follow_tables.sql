-- Create the trigger function
CREATE OR REPLACE FUNCTION check_dj_exists()
RETURNS TRIGGER AS $$
BEGIN
    -- Check if the dj_id exists in the dj table
    IF NOT EXISTS (SELECT 1 FROM dj WHERE id = NEW.dj_id) THEN
        RAISE EXCEPTION 'DJ with id % does not exist', NEW.dj_id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create the trigger on user_dj_followers table
CREATE TRIGGER validate_dj_id
BEFORE INSERT ON user_dj_followers
FOR EACH ROW
EXECUTE FUNCTION check_dj_exists();

-- Create the trigger function
CREATE OR REPLACE FUNCTION check_event_exists()
RETURNS TRIGGER AS $$
BEGIN
    -- Check if the dj_id exists in the dj table
    IF NOT EXISTS (SELECT 1 FROM event_data WHERE id = NEW.event_id) THEN
        RAISE EXCEPTION 'Event with id % does not exist', NEW.dj_id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create the trigger on user_dj_followers table
CREATE TRIGGER validate_event_id
BEFORE INSERT ON user_event_followers
FOR EACH ROW
EXECUTE FUNCTION check_event_exists();

-- Trigger for incrementing num_saves after an INSERT
CREATE TRIGGER after_user_event_follow_insert
AFTER INSERT ON user_event_followers
FOR EACH ROW
EXECUTE FUNCTION increment_num_saves();

CREATE OR REPLACE FUNCTION increment_num_saves()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE event_data
    SET num_saves = num_saves + 1
    WHERE id = NEW.event_id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;




-- Trigger for decrementing num_saves after a DELETE
CREATE TRIGGER after_user_event_follow_delete
AFTER DELETE ON user_event_followers
FOR EACH ROW
EXECUTE FUNCTION decrement_num_saves();

CREATE OR REPLACE FUNCTION decrement_num_saves()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE event_data
    SET num_saves = num_saves - 1
    WHERE id = OLD.event_id;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;






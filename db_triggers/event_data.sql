-- TRIGGER 1
CREATE TRIGGER check_event_completion AFTER INSERT OR UPDATE ON public.event_data 
FOR EACH ROW EXECUTE FUNCTION update_all_completed_status()

-- Function 1
CREATE OR REPLACE FUNCTION public.update_all_completed_status()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
    -- Update the completed status for all relevant events
    UPDATE published_events pe
    SET completed = TRUE
    FROM event_data ed
    WHERE pe.event_id = ed.id
    AND ed.date < NOW()
    AND pe.completed = FALSE;
    
    RETURN NEW;
END;
$function$
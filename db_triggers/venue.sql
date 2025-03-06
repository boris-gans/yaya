CREATE OR REPLACE FUNCTION public.update_venue_metric_col()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
BEGIN
    NEW.metrics := jsonb_build_object(
        'capacity', COALESCE(NEW.capacity, 0),
        'city', COALESCE(NEW.city, 0),
        'num_events', COALESCE(NEW.completed_events_count, 0),
        'gender_ratio', COALESCE(NEW.gender_ratio, 0),
        'avg_age', COALESCE(NEW.avg_age, 0),
        'avg_fill_ratio', COALESCE(NEW.avg_fill_ratio, 0),
        'avg_ctr', COALESCE(NEW.avg_ctr, 0),
        'avg_conversion_rate', COALESCE(NEW.avg_conversion_rate, 0),
        'avg_table_rate', COALESCE(NEW.avg_table_rate, 0),
        'avg_ticket_revenue', COALESCE(NEW.avg_ticket_revenue, 0),
        'avg_table_revenue', COALESCE(NEW.avg_table_revenue, 0)
    );

    RETURN NEW;
END;
$function$;
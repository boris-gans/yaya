-- AGGREGATES METRICS INTO JSON FOR EXPORT
    -- TRIGGER 1
    CREATE TRIGGER update_dj_metric_col_trigger BEFORE UPDATE ON public.dj 
    FOR EACH ROW EXECUTE FUNCTION update_dj_metric_col()

    -- FUNCTION 1
    CREATE OR REPLACE FUNCTION public.update_dj_metric_col()
    RETURNS trigger
    LANGUAGE plpgsql
    AS $function$
    BEGIN

    NEW.metrics := jsonb_build_object(
            'num_events', COALESCE(NEW.completed_events_count, 0),
            'num_org_chats', (SELECT COUNT(*) FROM dj_org_chats WHERE dj_id = NEW.id),
            'monthly_streams', COALESCE(NEW.monthly_streams, 0),
            'native_followers', COALESCE(NEW.interested_count, 0),
            'social_followers', COALESCE(NEW.social_followers, 0),
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
    $function$
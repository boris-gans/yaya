-- UPDATES COLUMNS IN DJ ONCE EVENT COMPLETED
    -- TRIGGER 1
    CREATE TRIGGER trg_update_dj_metrics
    AFTER INSERT ON completed_event_metrics
    FOR EACH ROW
    EXECUTE FUNCTION update_dj_metrics();

    -- FUNCTION 1
    CREATE OR REPLACE FUNCTION update_dj_metrics()
    RETURNS TRIGGER AS $$
    DECLARE
        dj_avg RECORD;
        language_dist JSONB;
    BEGIN
        -- Iterate over each DJ linked to the completed event
        FOR dj_avg IN 
            SELECT 
                ed.dj_id, 
                AVG(cem.fill_ratio) AS avg_fill_ratio,
                AVG(cem.ctr) AS avg_ctr,
                AVG(cem.conversion_rate) AS avg_conversion_rate,
                AVG(cem.table_rate) AS avg_table_rate,
                AVG(cem.ticket_revenue) AS avg_ticket_revenue,
                AVG(cem.table_revenue) AS avg_table_revenue,
                AVG(cem.avg_age) AS avg_avg_age,
                AVG(cem.gender_ratio) AS avg_gender_ratio,
                AVG(cem.english_ratio) AS avg_english_ratio,
                AVG(cem.spanish_ratio) AS avg_spanish_ratio,
                AVG(cem.dutch_ratio) AS avg_dutch_ratio
            FROM completed_event_metrics cem
            JOIN event_dj ed ON cem.event_id = ed.event_id
            WHERE cem.event_id = NEW.event_id  -- Process only for the new event
            GROUP BY ed.dj_id
        LOOP
            -- Construct the language_distribution JSONB object
            language_dist := jsonb_build_object(
                'english_ratio', dj_avg.avg_english_ratio,
                'spanish_ratio', dj_avg.avg_spanish_ratio,
                'dutch_ratio', dj_avg.avg_dutch_ratio
            );

            -- Update the DJ's aggregated statistics in the DJ table
            UPDATE dj
            SET 
                avg_fill_ratio = dj_avg.avg_fill_ratio,
                avg_ctr = dj_avg.avg_ctr,
                avg_conversion_rate = dj_avg.avg_conversion_rate,
                avg_table_rate = dj_avg.avg_table_rate,
                avg_ticket_revenue = dj_avg.avg_ticket_revenue,
                avg_table_revenue = dj_avg.avg_table_revenue,
                avg_age = dj_avg.avg_avg_age,
                gender_ratio = dj_avg.avg_gender_ratio,
                language_distribution = language_dist  -- Update the JSONB field
            WHERE dj.id = dj_avg.dj_id;
        END LOOP;

        PERFORM update_venue_metrics();

        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;

    -- FUNCTION 1B
    CREATE OR REPLACE FUNCTION update_venue_metrics()
    RETURNS VOID AS $$
    DECLARE
        venue_avg RECORD;
        language_dist JSONB;
    BEGIN
        FOR venue_avg IN 
            SELECT 
            	e.venue_id,
                AVG(cem.fill_ratio) AS avg_fill_ratio,
                AVG(cem.ctr) AS avg_ctr,
                AVG(cem.conversion_rate) AS avg_conversion_rate,
                AVG(cem.table_rate) AS avg_table_rate,
                AVG(cem.ticket_revenue) AS avg_ticket_revenue,
                AVG(cem.table_revenue) AS avg_table_revenue,
                AVG(cem.avg_age) AS avg_avg_age,
                AVG(cem.gender_ratio) AS avg_gender_ratio,
                AVG(cem.english_ratio) AS avg_english_ratio,
                AVG(cem.spanish_ratio) AS avg_spanish_ratio,
                AVG(cem.dutch_ratio) AS avg_dutch_ratio
            FROM completed_event_metrics cem
            JOIN event_data e ON cem.event_id = e.id
            WHERE e.venue_id IS NOT NULL
            GROUP BY e.venue_id
        LOOP
            -- Construct the language_distribution JSONB object
            language_dist := jsonb_build_object(
                'english_ratio', venue_avg.avg_english_ratio,
                'spanish_ratio', venue_avg.avg_spanish_ratio,
                'dutch_ratio', venue_avg.avg_dutch_ratio
            );

            -- Update the venue's aggregated statistics in the venues table
            UPDATE venues
            SET 
                avg_fill_ratio = venue_avg.avg_fill_ratio,
                avg_ctr = venue_avg.avg_ctr,
                avg_conversion_rate = venue_avg.avg_conversion_rate,
                avg_table_rate = venue_avg.avg_table_rate,
                avg_ticket_revenue = venue_avg.avg_ticket_revenue,
                avg_table_revenue = venue_avg.avg_table_revenue,
                avg_age = venue_avg.avg_avg_age,
                gender_ratio = venue_avg.avg_gender_ratio,
                language_distribution = language_dist
            WHERE venues.id = venue_avg.venue_id;
        END LOOP;
    END;
    $$ LANGUAGE plpgsql;


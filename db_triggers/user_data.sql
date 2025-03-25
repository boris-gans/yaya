-- TRIGGER 1
CREATE TRIGGER initialize_user_data_fields_trigger BEFORE INSERT OR UPDATE ON public.user_data 
FOR EACH ROW EXECUTE FUNCTION initialize_user_data_fields()

-- FUNCTION 1
CREATE OR REPLACE FUNCTION public.initialize_user_data_fields()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
    -- Initialize core with relevant values
    NEW.core = to_jsonb(
        json_build_object(
            'age', EXTRACT(YEAR FROM AGE(NEW.birthdate)),
            'gender', NEW.gender,
            'language', NEW.language,
            'country', NEW.country,
            'city', NEW.city
        )
    );
    RETURN NEW;
END;
$function$

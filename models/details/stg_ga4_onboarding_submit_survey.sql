{{ config(schema='ga4' if target.name == 'prod' else 'mc_data_statistics') }} 

{{ ga4.create_custom_event('onboarding_submit_survey') }}
{{ config(materialized='view') }}

with 

collegedata as (

    select * from {{ source('staging', 'merged_data_1920') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['STABBR', 'REGION']) }} as collegeid,
        {{ dbt.safe_cast("unitid", api.Column.translate_type("integer")) }} as unitid,
        {{ dbt.safe_cast("opeid", api.Column.translate_type("integer")) }} as opeid,
        {{ dbt.safe_cast("opeid6", api.Column.translate_type("integer")) }} as opeid6,
        instnm,
        city,
        stabbr,
        zip,
        main,
        numbranch,
        preddeg,
        coalesce({{ dbt.safe_cast("preddeg", api.Column.translate_type("integer")) }},0) as pdegree_type_description,
        {{get_degree_type_description('preddeg')}} as degree_type_description,
        highdeg,
        control,
        st_fips,
        region,
        iclevel,
        opeflag,
        __index_level_0__
    from collegedata

)

select * from renamed

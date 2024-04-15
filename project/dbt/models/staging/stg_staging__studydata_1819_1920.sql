with 

source as (

    select * from {{ source('staging', 'studydata_1819_1920') }}

),

renamed as (

    select
        unitid,
        opeid6,
        instnm,
        control,
        main,
        cipcode,
        cipdesc,
        credlev,
        creddesc,
        distance,
        __index_level_0__

    from source

)

select * from renamed

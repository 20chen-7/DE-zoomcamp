{{ config(materialized='view') }}

with 

collegedata as (

    select * from {{ source('staging', 'merged_data_1920') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['unitid', 'st_fips']) }} as collegeid,
        {{ dbt.safe_cast("unitid", api.Column.translate_type("integer")) }} as unitid,
        {{ dbt.safe_cast("opeid", api.Column.translate_type("integer")) }} as opeid,
        {{ dbt.safe_cast("opeid6", api.Column.translate_type("integer")) }} as opeid6,
        instnm,
        city,
        stabbr,
        zip,
        main,
        numbranch,
        coalesce({{ dbt.safe_cast("preddeg", api.Column.translate_type("integer")) }},0) as preddeg,
        {{get_degree_type_description('preddeg')}} as degree_type_description,
        highdeg,
        control,
        st_fips,
        region,
        iclevel,
        opeflag, 
        sch_deg,
        adm_rate,
        adm_rate_all,
        satvr25,
        satvr75,
        satmt25,
        satmt75,
        satvrmid,
        satmtmid,
        actcm25,
        actcm75,
        acten25,
        acten75,
        actmt25,
        actmt75,
        actcmmid,
        actenmid,
        actmtmid,
        sat_avg,
        sat_avg_all,
        pcip01,
        pcip03,
        pcip04,
        pcip05,
        pcip09,
        pcip10,
        pcip11,
        pcip12,
        pcip13,
        pcip14,
        pcip15,
        pcip16,
        pcip19,
        pcip22,
        pcip23,
        pcip24,
        pcip25,
        pcip26,
        pcip27,
        pcip29,
        pcip30,
        pcip31,
        pcip38,
        pcip39,
        pcip40,
        pcip41,
        pcip42,
        pcip43,
        pcip44,
        pcip45,
        pcip46,
        pcip47,
        pcip48,
        pcip49,
        pcip50,
        pcip51,
        pcip52,
        pcip54,
        cip01cert1,
        cip01cert2,
        cip01assoc,
        cip01cert4,
        cip01bachl,
        cip03cert1,
        cip03cert2,
        cip03assoc,
        cip03cert4,
        cip03bachl,
        cip04cert1,
        cip04cert2,
        cip04assoc,
        cip04cert4,
        cip04bachl,
        cip05cert1,
        cip05cert2,
        cip05assoc,
        cip05cert4,
        cip05bachl,
        cip09cert1,
        cip09cert2,
        cip09assoc,
        cip09cert4,
        cip09bachl,
        cip10cert1,
        cip10cert2,
        cip10assoc,
        cip10cert4,
        cip10bachl,
        cip11cert1,
        cip11cert2,
        cip11assoc,
        cip11cert4,
        cip11bachl,
        cip12cert1,
        cip12cert2,
        cip12assoc,
        cip12cert4,
        cip12bachl,
        cip13cert1,
        cip13cert2,
        cip13assoc,
        cip13cert4,
        cip13bachl,
        cip14cert1,
        cip14cert2,
        cip14assoc,
        cip14cert4,
        cip14bachl,
        cip15cert1,
        cip15cert2,
        cip15assoc,
        cip15cert4,
        cip15bachl,
        cip16cert1,
        cip16cert2,
        cip16assoc,
        cip16cert4,
        cip16bachl,
        cip19cert1,
        cip19cert2,
        cip19assoc,
        cip19cert4,
        cip19bachl,
        cip22cert1,
        cip22cert2,
        cip22assoc,
        cip22cert4,
        cip22bachl,
        cip23cert1,
        cip23cert2,
        cip23assoc,
        cip23cert4,
        cip23bachl,
        cip24cert1,
        cip24cert2,
        cip24assoc,
        cip24cert4,
        cip24bachl,
        cip25cert1,
        cip25cert2,
        cip25assoc,
        cip25cert4,
        cip25bachl,
        cip26cert1,
        cip26cert2,
        cip26assoc,
        cip26cert4,
        cip26bachl,
        cip27cert1,
        cip27cert2,
        cip27assoc,
        cip27cert4,
        cip27bachl,
        cip29cert1,
        cip29cert2,
        cip29assoc,
        cip29cert4,
        cip29bachl,
        cip30cert1,
        cip30cert2,
        cip30assoc,
        cip30cert4,
        cip30bachl,
        cip31cert1,
        cip31cert2,
        cip31assoc,
        cip31cert4,
        cip31bachl,
        cip38cert1,
        cip38cert2,
        cip38assoc,
        cip38cert4,
        cip38bachl,
        cip39cert1,
        cip39cert2,
        cip39assoc,
        cip39cert4,
        cip39bachl,
        cip40cert1,
        cip40cert2,
        cip40assoc,
        cip40cert4,
        cip40bachl,
        cip41cert1,
        cip41cert2,
        cip41assoc,
        cip41cert4,
        cip41bachl,
        cip42cert1,
        cip42cert2,
        cip42assoc,
        cip42cert4,
        cip42bachl,
        cip43cert1,
        cip43cert2,
        cip43assoc,
        cip43cert4,
        cip43bachl,
        cip44cert1,
        cip44cert2,
        cip44assoc,
        cip44cert4,
        cip44bachl,
        cip45cert1,
        cip45cert2,
        cip45assoc,
        cip45cert4,
        cip45bachl,
        cip46cert1,
        cip46cert2,
        cip46assoc,
        cip46cert4,
        cip46bachl,
        cip47cert1,
        cip47cert2,
        cip47assoc,
        cip47cert4,
        cip47bachl,
        cip48cert1,
        cip48cert2,
        cip48assoc,
        cip48cert4,
        cip48bachl,
        cip49cert1,
        cip49cert2,
        cip49assoc,
        cip49cert4,
        cip49bachl,
        cip50cert1,
        cip50cert2,
        cip50assoc,
        cip50cert4,
        cip50bachl,
        cip51cert1,
        cip51cert2,
        cip51assoc,
        cip51cert4,
        cip51bachl,
        cip52cert1,
        cip52cert2,
        cip52assoc,
        cip52cert4,
        cip52bachl,
        cip54cert1,
        cip54cert2,
        cip54assoc,
        cip54cert4,
        cip54bachl,
        distanceonly,
        ugds,
        ugds_white,
        ugds_black,
        ugds_hisp,
        ugds_asian,
        ugds_aian,
        ugds_nhpi,
        ugds_2mor,
        ugds_nra,
        ugds_unkn,
        pptug_ef,
        npt4_pub,
        npt4_priv,
        npt41_pub,
        npt42_pub,
        npt43_pub,
        npt44_pub,
        npt45_pub,
        npt41_priv,
        npt42_priv,
        npt43_priv,
        npt44_priv,
        npt45_priv,
        npt4_048_pub,
        npt4_048_priv,
        npt4_3075_pub,
        npt4_3075_priv,
        npt4_75up_pub,
        npt4_75up_priv,
        num4_pub,
        num4_priv,
        num41_pub,
        num42_pub,
        num43_pub,
        num44_pub,
        num45_pub,
        num41_priv,
        num42_priv,
        num43_priv,
        num44_priv,
        num45_priv,
        costt4_a,
        costt4_p,
        tuitionfee_in,
        tuitionfee_out,
        tuitionfee_prog,
        tuitfte,
        inexpfte,
        avgfacsal,
        pftfac,
        pctpell,
        c150_4,
        c150_l4,
        pftftug1_ef,
        d150_4,
        d150_l4,
        c150_4_white,
        c150_4_black,
        c150_4_hisp,
        c150_4_asian,
        c150_4_aian,
        c150_4_nhpi,
        c150_4_2mor,
        c150_4_nra,
        c150_4_unkn,
        c150_l4_white,
        c150_l4_black,
        c150_l4_hisp,
        c150_l4_asian,
        c150_l4_aian,
        c150_l4_nhpi,
        c150_l4_2mor,
        c150_l4_nra,
        c150_l4_unkn,
        c200_4,
        c200_l4,
        d200_4,
        d200_l4,
        ret_ft4,
        ret_ftl4,
        ret_pt4,
        ret_ptl4,
        pctfloan,
        ug25abv,
        cdr3,
        count_nwne_p10,
        count_wne_p10,
        md_earn_wne_p10,
        pct25_earn_wne_p10,
        pct75_earn_wne_p10,
        sd_earn_wne_p10,
        count_wne_inc1_p10,
        count_wne_inc2_p10,
        count_wne_inc3_p10,
        count_wne_indep0_p10,
        count_wne_indep1_p10,
        count_wne_male0_p10,
        count_wne_male1_p10,
        count_nwne_p6,
        count_wne_p6,
        md_earn_wne_p6,
        pct25_earn_wne_p6,
        pct75_earn_wne_p6,
        sd_earn_wne_p6,
        count_wne_inc1_p6,
        count_wne_inc2_p6,
        count_wne_inc3_p6,
        count_wne_indep0_p6,
        count_wne_indep1_p6,
        count_wne_male0_p6,
        count_wne_male1_p6,
        count_nwne_p8,
        count_wne_p8,
        md_earn_wne_p8,
        pct25_earn_wne_p8,
        pct75_earn_wne_p8,
        sd_earn_wne_p8,
        alias,
        c100_4,
        d100_4,
        c100_l4,
        d100_l4,
        trans_4,
        dtrans_4,
        trans_l4,
        dtrans_l4,
        ugds_men,
        ugds_women,
        cdr3_denom,
        d150_4_white,
        d150_4_black,
        d150_4_hisp,
        d150_4_asian,
        d150_4_aian,
        d150_4_nhpi,
        d150_4_2mor,
        d150_4_nra,
        d150_4_unkn,
        d150_l4_white,
        d150_l4_black,
        d150_l4_hisp,
        d150_l4_asian,
        d150_l4_aian,
        d150_l4_nhpi,
        d150_l4_2mor,
        d150_l4_nra,
        d150_l4_unkn,
        d_pctpell_pctfloan,
        openadmp,
        ugnonds,
        grads,
        omacht6_ftft,
        omawdp6_ftft,
        omacht8_ftft,
        omawdp8_ftft,
        omenryp8_ftft,
        omenrap8_ftft,
        omenrup8_ftft,
        omacht6_ptft,
        omawdp6_ptft,
        omacht8_ptft,
        omawdp8_ptft,
        omenryp8_ptft,
        omenrap8_ptft,
        omenrup8_ptft,
        omacht6_ftnft,
        omawdp6_ftnft,
        omacht8_ftnft,
        omawdp8_ftnft,
        omenryp8_ftnft,
        omenrap8_ftnft,
        omenrup8_ftnft,
        omacht6_ptnft,
        omawdp6_ptnft,
        omacht8_ptnft,
        omawdp8_ptnft,
        omenryp8_ptnft,
        omenrap8_ptnft,
        omenrup8_ptnft,
        c150_4_pell,
        d150_4_pell,
        c150_l4_pell,
        d150_l4_pell,
        c150_4_loannopell,
        d150_4_loannopell,
        c150_l4_loannopell,
        d150_l4_loannopell,
        c150_4_noloannopell,
        d150_4_noloannopell,
        c150_l4_noloannopell,
        d150_l4_noloannopell,
        prgmofr,
        cipcode1,
        cipcode2,
        cipcode3,
        cipcode4,
        cipcode5,
        cipcode6,
        ciptitle1,
        ciptitle2,
        ciptitle3,
        ciptitle4,
        ciptitle5,
        ciptitle6,
        ciptfbs1,
        ciptfbs2,
        ciptfbs3,
        ciptfbs4,
        ciptfbs5,
        ciptfbs6,
        ciptfbsannual1,
        ciptfbsannual2,
        ciptfbsannual3,
        ciptfbsannual4,
        ciptfbsannual5,
        ciptfbsannual6,
        mthcmp1,
        mthcmp2,
        mthcmp3,
        mthcmp4,
        mthcmp5,
        mthcmp6,
        omenryp_all,
        omenrap_all,
        omawdp8_all,
        omenrup_all,
        omenryp_firsttime,
        omenrap_firsttime,
        omawdp8_firsttime,
        omenrup_firsttime,
        omenryp_notfirsttime,
        omenrap_notfirsttime,
        omawdp8_notfirsttime,
        omenrup_notfirsttime,
        omenryp_fulltime,
        omenrap_fulltime,
        omawdp8_fulltime,
        omenrup_fulltime,
        omenryp_parttime,
        omenrap_parttime,
        omawdp8_parttime,
        omenrup_parttime,
        ftftpctpell,
        ftftpctfloan,
        ug12mn,
        g12mn,
        scugffn,
        pplus_pct_low,
        pplus_pct_high,
        booksupply,
        roomboard_on,
        otherexpense_on,
        roomboard_off,
        otherexpense_off,
        otherexpense_fam,
        endowbegin,
        endowend,
        admcon7,
        omenryp_pell_all,
        omenrap_pell_all,
        omawdp8_pell_all,
        omenrup_pell_all,
        omenryp_pell_firsttime,
        omenrap_pell_firsttime,
        omawdp8_pell_firsttime,
        omenrup_pell_firsttime,
        omenryp_pell_notfirsttime,
        omenrap_pell_notfirsttime,
        omawdp8_pell_notfirsttime,
        omenrup_pell_notfirsttime,
        omenryp_pell_fulltime,
        omenrap_pell_fulltime,
        omawdp8_pell_fulltime,
        omenrup_pell_fulltime,
        omenryp_pell_parttime,
        omenrap_pell_parttime,
        omawdp8_pell_parttime,
        omenrup_pell_parttime,
        omacht8_pell_ftft,
        omawdp8_pell_ftft,
        omenryp8_pell_ftft,
        omenrap8_pell_ftft,
        omenrup8_pell_ftft,
        omacht8_pell_ptft,
        omawdp8_pell_ptft,
        omenryp8_pell_ptft,
        omenrap8_pell_ptft,
        omenrup8_pell_ptft,
        omacht8_pell_ftnft,
        omawdp8_pell_ftnft,
        omenryp8_pell_ftnft,
        omenrap8_pell_ftnft,
        omenrup8_pell_ftnft,
        omacht8_pell_ptnft,
        omawdp8_pell_ptnft,
        omenryp8_pell_ptnft,
        omenrap8_pell_ptnft,
        omenrup8_pell_ptnft,
        count_nwne_1yr,
        count_wne_1yr,
        gt_threshold_p6,
        md_earn_wne_inc1_p6,
        md_earn_wne_inc2_p6,
        md_earn_wne_inc3_p6,
        md_earn_wne_indep1_p6,
        md_earn_wne_indep0_p6,
        md_earn_wne_male0_p6,
        md_earn_wne_male1_p6,
        gt_threshold_p8,
        count_wne_inc1_p8,
        md_earn_wne_inc1_p8,
        count_wne_inc2_p8,
        md_earn_wne_inc2_p8,
        count_wne_inc3_p8,
        md_earn_wne_inc3_p8,
        count_wne_indep1_p8,
        md_earn_wne_indep1_p8,
        count_wne_indep0_p8,
        md_earn_wne_indep0_p8,
        count_wne_male0_p8,
        md_earn_wne_male0_p8,
        count_wne_male1_p8,
        md_earn_wne_male1_p8,
        gt_threshold_p10,
        md_earn_wne_inc1_p10,
        md_earn_wne_inc2_p10,
        md_earn_wne_inc3_p10,
        md_earn_wne_indep1_p10,
        md_earn_wne_indep0_p10,
        md_earn_wne_male0_p10,
        md_earn_wne_male1_p10,
        stufacr,
        irps_2mor,
        irps_aian,
        irps_asian,
        irps_black,
        irps_hisp,
        irps_nhpi,
        irps_nra,
        irps_unkn,
        irps_white,
        irps_women,
        irps_men,
        md_earn_wne_1yr,
        gt_threshold_1yr,
        count_nwne_4yr,
        count_wne_4yr,
        md_earn_wne_4yr,
        gt_threshold_4yr,
        omenryp_nopell_all,
        omenrap_nopell_all,
        omawdp8_nopell_all,
        omenrup_nopell_all,
        omenryp_nopell_firsttime,
        omenrap_nopell_firsttime,
        omawdp8_nopell_firsttime,
        omenrup_nopell_firsttime,
        omenryp_nopell_notfirsttime,
        omenrap_nopell_notfirsttime,
        omawdp8_nopell_notfirsttime,
        omenrup_nopell_notfirsttime,
        omacht8_nopell_all,
        omacht8_nopell_firsttime,
        omacht8_nopell_notfirsttime,
        __index_level_0__
        __index_level_0__
    from collegedata

)

select * from renamed

-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}

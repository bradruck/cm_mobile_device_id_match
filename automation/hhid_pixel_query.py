# hhid_pixel_query module
# Module holds the class => MaidHHIDMatch - manages Hive query template
# Class responsible to populate the query with api sourced variables
#
from datetime import datetime


class MaidHHIDMatch(object):

    @staticmethod
    def unified_impressions_query(pixel, start_date):
        present_date = datetime.now().strftime('%Y%m%d')
        query = """
        set hive.execution.engine = tez;
        set fs.s3n.block.size=128000000;
        set fs.s3a.block.size=128000000;

        select 'hashed' as type,
        count(dlx_chpck) as dlx_chpck,
        count(b.hhid) as hhid
        from (select dlx_chpck 
        from core_digital.unified_impression
        WHERE PIXEL_ID IN ({pixel})
        AND DATA_DATE >= ({start_date})
        AND DATA_SOURCE_ID_PART = '6'
        AND SOURCE = 'save'
        and length(dlx_chpck) in (32,40,64)
        ) a
        left join identity.maid_ind_with_hashes b
        on lower(a.dlx_chpck)=lower(b.maid)

        union all 

        select 'un-hashed' as type,
        count(dlx_chpck) as dlx_chpck,
        count(b.hhid) as hhid
        from (select dlx_chpck 
        from core_digital.unified_impression
        WHERE PIXEL_ID IN ({pixel})
        AND DATA_DATE >= ({start_date})
        AND DATA_SOURCE_ID_PART = '6'
        AND SOURCE = 'save'
        and length(dlx_chpck)=36
        ) a
        left join identity.maid_ind b
        on lower(a.dlx_chpck)=lower(b.maid)

        union all

        select 'cookie' as type,
        count(na_guid_id) as dlx_chpck,
        NVL(sum(case when hhid<>0 then 1 else 0 end), 0) as hhid
        from (select na_guid_id,
        hhid 
        from core_digital.unified_impression
        WHERE PIXEL_ID IN ({pixel})
        AND DATA_DATE >= ({start_date})
        AND DATA_SOURCE_ID_PART = '6'
        AND SOURCE = 'save'
        and length(dlx_chpck) not in (32,36,40,64)
        ) a
        """.format(start_date=start_date, pixel=pixel)
        return query

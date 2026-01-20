{{ config(
    materialized='incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by={
        'field': 'pt',
        'data_type': 'date'
    }
) }}


WITH RECURSIVE combined_records AS (
 SELECT id, record_id,resource_group_id,workspace_id, create_time,transcription_status  FROM `notta-data-analytics.notta_aurora.langogo_user_space_records0`
  UNION ALL
  SELECT id, record_id,resource_group_id,workspace_id, create_time,transcription_status  FROM `notta-data-analytics.notta_aurora.langogo_user_space_records1`
  UNION ALL
  SELECT id, record_id,resource_group_id,workspace_id, create_time,transcription_status  FROM `notta-data-analytics.notta_aurora.langogo_user_space_records2`
  UNION ALL
  SELECT id, record_id,resource_group_id,workspace_id, create_time,transcription_status  FROM `notta-data-analytics.notta_aurora.langogo_user_space_records3`
  UNION ALL
  SELECT id, record_id,resource_group_id,workspace_id,create_time,transcription_status FROM `notta-data-analytics.notta_aurora.langogo_user_space_records4`
  UNION ALL
  SELECT id, record_id,resource_group_id,workspace_id,create_time,transcription_status FROM `notta-data-analytics.notta_aurora.langogo_user_space_records5`
  UNION ALL
  SELECT id, record_id,resource_group_id,workspace_id,create_time,transcription_status FROM `notta-data-analytics.notta_aurora.langogo_user_space_records6`
  UNION ALL
  SELECT id, record_id,resource_group_id,workspace_id,create_time,transcription_status FROM `notta-data-analytics.notta_aurora.langogo_user_space_records7`
  UNION ALL
  SELECT id, record_id,resource_group_id,workspace_id,create_time,transcription_status FROM `notta-data-analytics.notta_aurora.langogo_user_space_records8`
  UNION ALL
  SELECT id, record_id,resource_group_id,workspace_id,create_time,transcription_status FROM `notta-data-analytics.notta_aurora.langogo_user_space_records9`
  UNION ALL
  SELECT id, record_id,resource_group_id,workspace_id,create_time,transcription_status FROM `notta-data-analytics.notta_aurora.langogo_user_space_records10`
  UNION ALL
  SELECT id, record_id,resource_group_id,workspace_id,create_time,transcription_status FROM `notta-data-analytics.notta_aurora.langogo_user_space_records11`
  UNION ALL
  SELECT id, record_id,resource_group_id,workspace_id,create_time,transcription_status FROM `notta-data-analytics.notta_aurora.langogo_user_space_records12`
  UNION ALL
  SELECT id, record_id,resource_group_id,workspace_id,create_time,transcription_status FROM `notta-data-analytics.notta_aurora.langogo_user_space_records13`
  UNION ALL
  SELECT id, record_id,resource_group_id,workspace_id,create_time,transcription_status FROM `notta-data-analytics.notta_aurora.langogo_user_space_records14`
  UNION ALL
  SELECT id, record_id,resource_group_id,workspace_id,create_time,transcription_status FROM `notta-data-analytics.notta_aurora.langogo_user_space_records15`
  UNION ALL
  SELECT id, record_id,resource_group_id,workspace_id,create_time,transcription_status FROM `notta-data-analytics.notta_aurora.langogo_user_space_records16`
  UNION ALL
  SELECT id, record_id,resource_group_id,workspace_id,create_time,transcription_status FROM `notta-data-analytics.notta_aurora.langogo_user_space_records17`
  UNION ALL
  SELECT id, record_id,resource_group_id,workspace_id,create_time,transcription_status FROM `notta-data-analytics.notta_aurora.langogo_user_space_records18`
  UNION ALL
  SELECT id, record_id,resource_group_id,workspace_id,create_time,transcription_status FROM `notta-data-analytics.notta_aurora.langogo_user_space_records19`
  UNION ALL
  SELECT id, record_id,resource_group_id,workspace_id,create_time,transcription_status FROM `notta-data-analytics.notta_aurora.langogo_user_space_records20`
  UNION ALL
  SELECT id, record_id,resource_group_id,workspace_id,create_time,transcription_status FROM `notta-data-analytics.notta_aurora.langogo_user_space_records21`
  UNION ALL
  SELECT id, record_id,resource_group_id,workspace_id,create_time,transcription_status FROM `notta-data-analytics.notta_aurora.langogo_user_space_records22`
  UNION ALL
  SELECT id, record_id,resource_group_id,workspace_id,create_time,transcription_status FROM `notta-data-analytics.notta_aurora.langogo_user_space_records23`
  UNION ALL
  SELECT id, record_id,resource_group_id,workspace_id,create_time,transcription_status FROM `notta-data-analytics.notta_aurora.langogo_user_space_records24`
  UNION ALL
  SELECT id, record_id,resource_group_id,workspace_id,create_time,transcription_status FROM `notta-data-analytics.notta_aurora.langogo_user_space_records25`
  UNION ALL
  SELECT id, record_id,resource_group_id,workspace_id,create_time,transcription_status FROM `notta-data-analytics.notta_aurora.langogo_user_space_records26`
  UNION ALL
  SELECT id, record_id,resource_group_id,workspace_id,create_time,transcription_status FROM `notta-data-analytics.notta_aurora.langogo_user_space_records27`
  UNION ALL
  SELECT id, record_id,resource_group_id,workspace_id,create_time,transcription_status FROM `notta-data-analytics.notta_aurora.langogo_user_space_records28`
  UNION ALL
  SELECT id, record_id,resource_group_id,workspace_id,create_time,transcription_status FROM `notta-data-analytics.notta_aurora.langogo_user_space_records29`
  UNION ALL
  SELECT id, record_id,resource_group_id,workspace_id,create_time,transcription_status FROM `notta-data-analytics.notta_aurora.langogo_user_space_records30`
  UNION ALL
  SELECT id, record_id,resource_group_id,workspace_id,create_time,transcription_status FROM `notta-data-analytics.notta_aurora.langogo_user_space_records31`
  UNION ALL
  SELECT id, record_id,resource_group_id,workspace_id,create_time,transcription_status FROM `notta-data-analytics.notta_aurora.langogo_user_space_records32`
),
agent_workspace_id_list as (SELECT workspace_id FROM `notta-data-analytics.notta_aurora.langogo_user_space_agents` where agent_status=1),

root_folders AS (
    -- 获取根文件夹
    SELECT resource_group_id
    FROM `notta-data-analytics.notta_aurora.langogo_user_space_agents` 
    WHERE workspace_id IN (select workspace_id from agent_workspace_id_list)),



folder_hierarchy AS (
    -- Base case: 获取直接子文件夹
    SELECT 
        resource_group_id,
        parent_id,
        1 as level
    FROM `notta-data-analytics.notta_aurora.langogo_user_space_resource_group`
    WHERE parent_id IN (
        SELECT resource_group_id
        FROM `notta-data-analytics.notta_aurora.langogo_user_space_agents` 
        WHERE workspace_id IN (select workspace_id from agent_workspace_id_list)
    )
    
    UNION ALL
    
    -- Recursive case: 获取子文件夹的子文件夹
    SELECT 
        child.resource_group_id,
        child.parent_id,
        parent.level + 1
    FROM `notta-data-analytics.notta_aurora.langogo_user_space_resource_group` child
    INNER JOIN folder_hierarchy parent
        ON child.parent_id = parent.resource_group_id
),
 workspace_agents AS (
   SELECT DISTINCT resource_group_id
FROM (
    -- 包含根文件夹
    SELECT resource_group_id
    FROM root_folders
    
    UNION ALL
    
    -- 包含所有子文件夹
    SELECT resource_group_id
    FROM folder_hierarchy
)),


record_id_list AS (
SELECT 
    cr.record_id
FROM workspace_agents wa
INNER JOIN combined_records cr 
    ON wa.resource_group_id = cr.resource_group_id
ORDER BY cr.create_time DESC),

attendee_result AS (
    SELECT workspace_id, record_id, attendee_source 
    FROM `notta-data-analytics.notta_aurora.langogo_user_space_attendees` 
    WHERE record_id IN (SELECT record_id FROM record_id_list)
),

group_attend AS (
    SELECT 
        workspace_id, 
        record_id, 
        COUNT(*) AS speaker_num 
    FROM attendee_result 
    GROUP BY workspace_id, record_id
    HAVING COUNT(*) > 0
)

SELECT workspace_id, COUNT(*) AS allocate_num ,  CURRENT_DATE() AS pt
FROM group_attend 
GROUP BY workspace_id 
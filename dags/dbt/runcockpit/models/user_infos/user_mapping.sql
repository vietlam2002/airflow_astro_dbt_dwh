{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='merge',
    merge_exclude_columns = ['createdAt'],
    post_hook=[
        """
        DO $$ 
        BEGIN 
            IF NOT EXISTS (
                SELECT 1 
                FROM information_schema.table_constraints 
                WHERE constraint_name = 'unique_user_mapping'
            ) THEN 
                ALTER TABLE {{ this }} 
                ADD CONSTRAINT unique_user_mapping UNIQUE (id); 
            END IF; 
        END $$;
        """
    ]
) }}


WITH _user_combines AS (
  SELECT *
  FROM {{ ref('_user_combines') }}
),
_create_users AS (
  SELECT
    _user_combines."id" AS "userId",
    2 AS "sourceType",
    _user_combines."createUserId" AS "userSourceId",
    NULL AS "lowerUserName",
    NULL AS "userKey"
  FROM
    _user_combines
),
_jira_users AS (
  SELECT
    _user_combines."id" AS "userId",
    1 AS "sourceType",
    CAST(_user_combines."jiraUserId" AS TEXT) AS "userSourceId",
    _user_combines."lowerUserName" AS "lowerUserName",
    _user_combines."userKey" AS "userKey"
  FROM
    _user_combines
), 
_user_mapping AS (
  SELECT
    *
  FROM
    _create_users
  UNION ALL
  SELECT
    *
  FROM
    _jira_users
)

SELECT
  DISTINCT ON ("userSourceId") MD5(CONCAT("userId", "userSourceId", "sourceType")) AS "id",
  _user_mapping.*,
  CURRENT_TIMESTAMP AS "createdAt",
  CURRENT_TIMESTAMP AS "updatedAt"
FROM
  _user_mapping
WHERE "userSourceId" IS NOT NULL
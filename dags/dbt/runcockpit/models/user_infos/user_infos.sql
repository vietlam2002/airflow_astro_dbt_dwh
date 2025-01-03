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
                WHERE constraint_name = 'unique_user_infos'
            ) THEN 
                ALTER TABLE {{ this }} 
                ADD CONSTRAINT unique_user_infos UNIQUE (id); 
            END IF; 
        END $$;
        """
    ]
) }}

WITH _user_combines AS (
  SELECT *
  FROM {{ ref('_user_combines') }}
)

SELECT
  DISTINCT ON ("id")
  _user_combines."id",
  _user_combines."name",
  (
    CASE
      WHEN _user_combines."staffcode" IS NOT NULL THEN _user_combines."staffcode"
      ELSE _cui."staffcode"
    END
  ) AS "staffCode",
  _user_combines."email",
  _cui."emailcompany",
  _cui."emailpersonal",
  _user_combines."userlevel",
  _user_combines."branchId",
  _user_combines."departmentId",
  _user_combines."positionId",
  TO_DATE(NULLIF(_cui."birthday", ''), 'DD/MM/YYYY')  AS "birthday",
  TO_DATE(NULLIF(_user_combines."welcomeDay", ''), 'YYYY-MM-DD') AS "welcomeday",
  CASE
    WHEN _cui."gender" = 'Nữ' THEN 0
    WHEN _cui."gender" = 'Nam' THEN 1
    ELSE NULL
  END AS "gender",
  _cui."mobile",
  _cui."address",
  (
    CASE
      WHEN _cui."isdeleted" IS NOT NULL THEN (
        CASE
          WHEN _cui."isdeleted" = 'Yes' THEN TRUE 
          ELSE FALSE
        END
        )
      ELSE _user_combines."isDeleted"
    END
  ) AS "isDeleted",
  TO_DATE(NULLIF(_cui."officialdate", ''), 'YYYY-MM-DD') AS "officialDate",
  TO_DATE(NULLIF(_cui."probationdate", ''), 'YYYY-MM-DD') AS "probationDate",
  TO_DATE(NULLIF(_cui."interndate", ''), 'YYYY-MM-DD') AS "internDate",
  TO_DATE(NULLIF(_cui."quitdate", ''), 'YYYY-MM-DD') AS "quitDate",
  CURRENT_TIMESTAMP AS "createdAt",
  CURRENT_TIMESTAMP AS "updatedAt"
FROM
  _user_combines
  LEFT JOIN data_lake."create_user_infos" _cui ON _user_combines."createUserId" = _cui."userobjid"
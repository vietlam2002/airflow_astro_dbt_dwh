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
                WHERE constraint_name = 'unique_user_positions'
            ) THEN 
                ALTER TABLE {{ this }} 
                ADD CONSTRAINT unique_user_positions UNIQUE (id); 
            END IF; 
        END $$;
        """
    ]
) }}

SELECT
  _user_positions."id",
  _user_positions."positionname" AS "name",
  _user_positions."positioncode" AS "code",
  _user_positions."positiondescription" AS "description",
  CASE 
    WHEN _user_positions."isdeleted" = 'Yes' THEN TRUE
    ELSE FALSE
  END AS "isdeleted",
  CURRENT_TIMESTAMP AS "createdat",
  CURRENT_TIMESTAMP AS "updatedat"
FROM
  data_lake."create_user_positions" _user_positions
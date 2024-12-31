WITH _jcu AS (
    SELECT 
        *,
        CASE
            WHEN _cwd."email_address" IS NOT NULL THEN _cwd."email_address"
            ELSE _app_user."lower_user_name"
        END AS "_email",
    FROM 
        data_lake."jira_app_user" _app_user
    LEFT JOIN data_lake."jira_cwd_user" _cwd ON _app_user."lower_user_name" = _cwd."lower_user_name"
),
_user_position AS (
    SELECT *
    FROM {{ ref('user_positions')}}
)

SELECT 
    DISTINCT ON (_icu."user_key", "name") MD5(
        CONCAT(
            _users."id",
            _jcu."ID",
            _jcu."user_key",
            _users."staffCode",
            "name"
        )
    ) AS "id",
    CASE 
        WHEN _users."name" IS NOT NULL THEN UPPER(_users."name")
        WHEN _jcu."lower_last_name" IS NOT NULL THEN UPPER(_jcu."lower_last_name")
        ELSE UPPER(
            SPLIT_PART(
                (
                    CASE
                        WHEN _users."email" IS NOT NULL THEN _users."email"
                        ELSE _icu."email"
                    END
                ),
                '@',
                1
            )
        )
    END AS "name",
    CASE
        WHEN _users."email" IS NOT NULL THEN _users."email"
        ELSE _jcu."email"
    END AS "email",
    _users."staffCode",
    _users."userLevel",
    _users."id" AS "createUserId",
    _jcu."ID" AS "jiraUserId",
    _jcu."_lower_user_name" AS "lowerUserName",
    _jcu."user_key" AS "userKey", 
    _users."branchObjId" AS "branchId",
    _users."departmentObjId" AS "departmentId",
    _users."positionObjId" AS "positionId",
    _users."userPositionObjId" AS "userPositionId",
    _users."welcomeDay" as "welcomeDay",
    CASE
        WHEN _jcu."deleted_externally" IS NOT NULL
        AND _jcu."deleted_externally" = 1 THEN TRUE
        WHEN _users."isDeleted" = 'Yes' THEN TRUE
        ELSE FALSE
    END AS "isDeleted"
FROM 
    data_lake."create_users" AS _users FULL
OUTER JOIN _jcu ON _jcu."email" = _users."email"
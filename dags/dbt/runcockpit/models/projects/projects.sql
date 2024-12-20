WITH _create as (
  SELECT
    *
  FROM
    public.create_projects cp
  WHERE
    LOWER(cp."jiraType") = 'jira8'
),
_jisseki AS (
  SELECT
    *
  FROM
    public.jisseki_projects prj
  WHERE
    prj.code IS NOT NULL
    AND prj.code != ''
)
SELECT
  DISTINCT ON (_jira.pkey) _jira.id AS "id",
  _jira.pkey AS "code",
  _create."projectName" AS "name",
  _create."projectType" AS "type",
  CONCAT_WS(
    ' ',
    _create."projectDescription",
    _jisseki.summary
  ) AS "description",
  CASE
    WHEN _jisseki.team_size = '' THEN NULL
    ELSE _jisseki.team_size
  END AS "teamSize",
  _jisseki.size AS "size",
  _jisseki.period AS "period",
  _jisseki.name_pm AS "namePM",
  _jisseki.name_br_se AS "nameBrSE",
  _jisseki."scope" AS "scope",
  _create."projectStatus" AS "status",
  _jisseki.amount AS "amount",
  _jisseki.point_css AS "pointCSS",
  _jisseki.startdate AS "startDate",
  _jisseki.enddate AS "endDate",
  CASE
    WHEN LOWER(_create."isDeleted") = 'no' THEN FALSE
    WHEN LOWER(_create."isDeleted") = 'yes' THEN TRUE
    ELSE FALSE
  END AS "isDeleted",
  CURRENT_TIMESTAMP AS "createdAt",
  CURRENT_TIMESTAMP AS "updatedAt"
FROM
  public.jira_project _jira
  left join _create on _jira."id" = _create."jiraProjectId"
  left join _jisseki on _create."projectCode" = _jisseki.code
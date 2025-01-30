*** Settings ***
# Resource                      ../resources/common.resource
Library                         ../libraries/SnowflakeConnector.py
...                             account=${account}
...                             user=${user}
...                             password=${password}
...                             warehouse=${warehouse}
...                             database=${database}
...                             schema=${schema}

*** Variables ***
${account}                      da03071.eu-north-1.aws
${user}                         ${EMPTY}  # add as CRT secret variables
${password}                     ${EMPTY}  # add as CRT secret variables
${warehouse}                    SANDBOX_WH
${database}                     SNOWFLAKE_SAMPLE_DATA
${schema}                       TPCH_SF1

*** Test Cases ***
Sample
    [Documentation]
    [Tags]

    SnowflakeConnector.Connect

    ${results}=                 SnowflakeConnector.Execute Query
    ...                         query=Select * from CUSTOMER limit 1

    SnowflakeConnector.Close

    Log To Console              ${results}
    # sample data response content
    # [{'C_CUSTKEY': 1, 'C_NAME': 'Customer#000000001', 'C_ADDRESS': 'IVhzIApeRb ot,c,E', 'C_NATIONKEY': 15, 'C_PHONE': '25-989-741-2988', 'C_ACCTBAL': Decimal('711.56'), 'C_MKTSEGMENT': 'BUILDING', 'C_COMMENT': 'to the even, regular platelets. regular, ironic epitaphs nag e'}]

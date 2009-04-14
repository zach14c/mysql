# We will disable the CSV storage engine if we include not_csv.inc
# in our test case.


--replace_result $ENGINE ENGINE
if(`SELECT '$ENGINE'='csv'`)
{
 skip "This test does not support the CSV engine";
}


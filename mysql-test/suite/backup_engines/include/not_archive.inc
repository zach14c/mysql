# We will disable the archive storage engine if we include not_archive.inc
# in our test case.


--replace_result $ENGINE ENGINE
if(`SELECT '$ENGINE'='archive'`)
{
 skip "This test does not support the archive engine";
}


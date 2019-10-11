import json

import requests

JITA_HOST = "https://jita.eng.nutanix.com"
USERNAME = "gokul.kannan"
PASSWORD = "Thi$isanfield"

def get_test_set(test_set_name):
    resp = requests.get(JITA_HOST + "/api/v2/test_sets", params={
        "raw_query": json.dumps({
            "name": test_set_name
        })
    }, timeout=60, allow_redirects=True)
    if resp and resp.ok:
        content = json.loads(resp.content)
        if content.get("success") and content.get("data") and content.get("total") == 1:
            return content["data"][0]

    raise Exception("Could bot find test set")

def update_test_set(test_set_name, test_name, resource_specs):
    test_set = get_test_set(test_set_name)
    test_set_id = test_set["_id"]["$oid"]
    tests = []
    for _test in test_set["tests"]:
        #if _test["name"] == test_name:
        _test["resource_specs"] = resource_specs
        _test["skip_test_refresh"] = True
        tests.append(_test)
    
    payload = {
        "tests": tests
    }

    url = JITA_HOST + "/api/v2/test_sets/" + test_set_id
    resp = requests.put(
        url, data=json.dumps(payload), auth=(USERNAME, PASSWORD),
        timeout=60, allow_redirects=True
    )
    if resp and resp.ok:
        content = json.loads(resp.content)
        if content.get("success"):
            return

    raise Exception("Could bot find test set")

if __name__ == "__main__":
    test_set_name = "cdp_regression_nested"
    test_name = "cdp.nos_smokes.test_smokes.TestCdpSmokes.test_smokes___minnode3"
    resource_specs = [{
        "type": "$NOS_CLUSTER",
        "hardware": {
            "min_host_gb_ram": 8,
            "cluster_min_nodes": 4
        }
    }]
    update_test_set(test_set_name, test_name, resource_specs)
